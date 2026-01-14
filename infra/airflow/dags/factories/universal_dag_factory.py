"""
Universal DAG Factory - Generates DAGs for any data source.
Supports CCXT, Fred, YFinance, and future sources with zero code changes.
"""

import logging
from datetime import datetime
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, Template

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class UniversalDAGFactory:
    """
    Factory for generating Airflow DAGs from configuration.
    Supports multiple data sources with a single template.
    """

    def __init__(
        self,
        config_path: str = "config/dags/dag_configs.yaml",
        template_dir: str = "infra/airflow/dags/templates",
        output_dir: str = "infra/airflow/dags/generated",
        sources_config_path: str | None = "config/bronze/sources.yaml",
        enforce_sources: bool = False,
    ):
        self.config_path = Path(config_path)
        self.template_dir = Path(template_dir)
        self.output_dir = Path(output_dir)
        self.sources_config_path = (
            Path(sources_config_path) if sources_config_path else None
        )
        self.enforce_sources = enforce_sources

        # Load configuration
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config not found: {self.config_path}")

        with open(self.config_path) as f:
            self.config = yaml.safe_load(f)

        self.sources_catalog = self._load_sources_catalog()

        # Setup Jinja environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.template_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Factory initialized: {self.config_path}")

    def _load_sources_catalog(self) -> dict[str, set[str]]:
        """Load source->data_type mapping from bronze sources config if present.

        Returns empty dict if the config is missing or unreadable to avoid blocking DAG generation.
        """
        catalog: dict[str, set[str]] = {}
        if not self.sources_config_path or not self.sources_config_path.exists():
            logger.info(
                "No sources catalog found; skipping source/data_type validation"
            )
            return catalog

        try:
            with open(self.sources_config_path) as f:
                data = yaml.safe_load(f) or {}
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Could not read sources catalog %s: %s", self.sources_config_path, exc
            )
            return catalog

        sources = data.get("sources") or []
        for entry in sources:
            source_id = entry.get("id") if isinstance(entry, dict) else None
            if not source_id:
                continue
            data_types = entry.get("data_types") if isinstance(entry, dict) else None
            if isinstance(data_types, list):
                catalog[source_id] = set(dt for dt in data_types if dt)
            else:
                catalog[source_id] = set()

        logger.info(
            "Loaded sources catalog (%d sources) from %s",
            len(catalog),
            self.sources_config_path,
        )
        return catalog

    def _is_supported_combo(self, data_source: str, data_type: str) -> bool:
        """Check whether the combo exists in sources catalog; empty catalog means allow all."""
        if not self.sources_catalog:
            return True

        allowed = self.sources_catalog.get(data_source)
        if allowed is None:
            return True

        # Empty set means allow all for that source
        if len(allowed) == 0:
            return True

        return data_type in allowed

    def generate_backfill_dags(self) -> int:
        """Generate all backfill DAGs from configuration."""
        generated_count = 0

        for dag_config in self.config.get("backfill_dags", []):
            if not dag_config.get("active", True):
                logger.info(f"Skipping inactive: {dag_config['data_type']}")
                continue

            data_type = dag_config["data_type"]
            data_source = dag_config["data_source"]
            exchanges = dag_config["exchanges"]
            timeframes = dag_config["timeframes"]

            if not self._is_supported_combo(data_source, data_type):
                msg = (
                    f"Config combo not in sources catalog: data_source={data_source}, "
                    f"data_type={data_type}"
                )
                if self.enforce_sources:
                    logger.warning(msg + " — skipping")
                    continue
                logger.warning(msg + " — continuing (enforce_sources=False)")

            logger.info(
                f"Generating backfill DAGs for {data_type} ({data_source}): "
                f"{len(exchanges)} exchanges × {len(timeframes)} timeframes"
            )

            for exchange in exchanges:
                for timeframe in timeframes:
                    try:
                        self._generate_single_dag(
                            dag_config=dag_config,
                            exchange=exchange,
                            timeframe=timeframe,
                            mode="backfill",
                        )
                        generated_count += 1

                    except Exception as e:
                        logger.error(
                            f"Failed to generate DAG for {exchange}/{timeframe}: {e}"
                        )

        return generated_count

    def generate_incremental_dags(self) -> int:
        """Generate all incremental update DAGs."""
        generated_count = 0

        for dag_config in self.config.get("incremental_dags", []):
            if not dag_config.get("active", True):
                continue

            data_type = dag_config["data_type"]
            data_source = dag_config["data_source"]
            exchanges = dag_config["exchanges"]
            timeframes = timeframes = dag_config["timeframes"]

            if not self._is_supported_combo(data_source, data_type):
                msg = (
                    f"Config combo not in sources catalog: data_source={data_source}, "
                    f"data_type={data_type}"
                )
                if self.enforce_sources:
                    logger.warning(msg + " — skipping")
                    continue
                logger.warning(msg + " — continuing (enforce_sources=False)")

            logger.info(
                f"Generating incremental DAGs for {data_type} ({data_source}): "
                f"{len(exchanges)} exchanges × {len(timeframes)} timeframes"
            )

            for exchange in exchanges:
                for timeframe in timeframes:
                    try:
                        self._generate_single_dag(
                            dag_config=dag_config,
                            exchange=exchange,
                            timeframe=timeframe,
                            mode="incremental",
                        )
                        generated_count += 1

                    except Exception as e:
                        logger.error(
                            f"Failed to generate incremental DAG for {exchange}/{timeframe}: {e}"
                        )

        return generated_count

    def _generate_single_dag(
        self, dag_config: dict, exchange: str, timeframe: str, mode: str
    ) -> None:
        """Generate a single DAG file from template."""
        data_type = dag_config["data_type"]
        data_source = dag_config["data_source"]

        # Build DAG ID
        dag_id = (
            f"{mode}_{data_type}_{data_source}_{exchange}_{timeframe.replace('/', '_')}"
        )

        # Build description
        description = (
            f"{data_type.upper()} {mode} via {data_source} "
            f"(Extract → DB) for {exchange} {timeframe}"
        )

        # Render SQL template
        sql_template_raw = dag_config["sql_template"]
        sql_jinja = Template(sql_template_raw)
        rendered_sql = sql_jinja.render(
            data_type=data_type, wait_seconds=dag_config.get("wait_seconds", 60)
        )

        # Determine output path
        output_subdir = self.output_dir / data_source / data_type
        output_subdir.mkdir(parents=True, exist_ok=True)
        output_path = (
            output_subdir / f"{mode}_{exchange}_{timeframe.replace('/', '_')}.py"
        )

        # Prepare template variables
        template_vars = {
            "dag_id": dag_id,
            "data_type": data_type,
            "data_source": data_source,
            "exchange": exchange,
            "timeframe": timeframe,
            "mode": mode,
            "description": description,
            "schedule": dag_config.get("schedule", None),
            "wait_seconds": dag_config.get("wait_seconds", 60),
            "success_threshold": dag_config.get("success_threshold", 90),
            "pool_slots": dag_config.get("pool_slots", 2),
            "execution_timeout_hours": dag_config.get("execution_timeout_hours", 4),
            "max_records_per_symbol": dag_config.get("max_records_per_symbol", 1500),
            "maintenance_lookback_days": dag_config.get("maintenance_lookback_days", 7),
            "rendered_sql": rendered_sql,
            "tags": [
                "backfill" if mode == "backfill" else "incremental",
                data_type,
                data_source,
                exchange,
                timeframe,
            ],
            "generation_time": datetime.utcnow().isoformat(),
        }

        # Render template
        template = self.jinja_env.get_template("universal_dag.j2")
        rendered = template.render(**template_vars)

        # Write to file
        with open(output_path, "w") as f:
            f.write(rendered)

        logger.info(f"✅ Generated: {output_path}")

    def generate_all(self) -> dict[str, int]:
        """Generate all configured DAGs."""
        results = {
            "backfill": self.generate_backfill_dags(),
            "incremental": self.generate_incremental_dags(),
        }

        total = sum(results.values())
        logger.info(f"✅ Total DAGs generated: {total}")
        logger.info(f"   - Backfill: {results['backfill']}")
        logger.info(f"   - Incremental: {results['incremental']}")

        return results

    def validate_generated_dags(self) -> bool:
        """
        Validate that generated DAGs have no syntax errors.
        Attempts to compile each Python file.
        """
        logger.info("Validating generated DAGs...")

        all_valid = True
        for dag_file in self.output_dir.rglob("*.py"):
            try:
                with open(dag_file) as f:
                    compile(f.read(), dag_file, "exec")
                logger.debug(f"✅ Valid: {dag_file.name}")
            except SyntaxError as e:
                logger.error(f"❌ Syntax error in {dag_file}: {e}")
                all_valid = False
            except Exception as e:
                logger.error(f"❌ Error validating {dag_file}: {e}")
                all_valid = False

        if all_valid:
            logger.info("✅ All generated DAGs are valid")
        else:
            logger.error("❌ Some DAGs have validation errors")

        return all_valid


def main():
    """CLI entry point for DAG generation."""
    import argparse

    parser = argparse.ArgumentParser(description="Generate Airflow DAGs")
    parser.add_argument(
        "--config",
        default="config/dags/dag_configs.yaml",
        help="Path to DAG configuration file",
    )
    parser.add_argument(
        "--sources-config",
        default="config/bronze/sources.yaml",
        help="Path to sources catalog for validation (source/data_type)",
    )
    parser.add_argument(
        "--enforce-sources",
        action="store_true",
        help="Skip generation when data_source/data_type not present in sources catalog",
    )
    parser.add_argument(
        "--validate", action="store_true", help="Validate generated DAGs after creation"
    )
    parser.add_argument(
        "--mode",
        choices=["backfill", "incremental", "all"],
        default="all",
        help="Which type of DAGs to generate",
    )

    args = parser.parse_args()

    factory = UniversalDAGFactory(
        config_path=args.config,
        sources_config_path=args.sources_config,
        enforce_sources=args.enforce_sources,
    )

    # Generate DAGs
    if args.mode == "backfill":
        count = factory.generate_backfill_dags()
    elif args.mode == "incremental":
        count = factory.generate_incremental_dags()
    else:
        results = factory.generate_all()
        count = sum(results.values())

    # Validate if requested
    if args.validate:
        is_valid = factory.validate_generated_dags()
        if not is_valid:
            exit(1)

    logger.info(f"✅ DAG generation complete: {count} DAGs created")


if __name__ == "__main__":
    main()
