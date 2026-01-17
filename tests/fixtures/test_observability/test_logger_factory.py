"""
Testes para as fábricas de loggers do módulo de observabilidade.

Estes testes verificam que:
1. get_logger() funciona corretamente com diferentes parâmetros
2. Todas as fábricas específicas por camada funcionam
3. Context binding e isolation funcionam como esperado
"""

import json
import logging
from io import StringIO

import pytest
import structlog

from quant_framework.infrastructure.observability import (
    get_database_logger,
    get_infrastructure_logger,
    get_ingestion_logger,
    get_logger,
    get_orchestration_logger,
    get_processing_logger,
    get_storage_logger,
    get_transformation_logger,
    setup_logging,
)


# Fixture para limpar e configurar logging
@pytest.fixture
def configured_logging():
    """
    Fixture que limpa e configura logging para testes de loggers.
    """
    # Limpar tudo
    logging.root.handlers = []
    structlog.reset_defaults()

    # Configurar em modo JSON para testes consistentes
    setup_logging(level="INFO", json_logs=True, include_timestamp=True)

    # Garantir nível INFO
    logging.root.setLevel(logging.INFO)

    yield

    # Limpar depois
    logging.root.handlers = []
    structlog.reset_defaults()


def capture_log_output(logger_callable, *args, **kwargs):
    """
    Helper para capturar output de uma chamada de logger.

    Args:
        logger_callable: Função que retorna um logger (ex: get_logger)
        *args, **kwargs: Argumentos para passar para logger_callable

    Returns:
        (logger, output_string)
    """
    # Criar buffer para captura
    captured_output = StringIO()

    # Adicionar handler
    handler = logging.StreamHandler(captured_output)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logging.root.addHandler(handler)

    try:
        # Obter logger
        logger = logger_callable(*args, **kwargs)

        # Emitir log de teste
        logger.info("test_event", test_value="capture")
        handler.flush()

        output = captured_output.getvalue().strip()
        return logger, output

    finally:
        logging.root.removeHandler(handler)


class TestGetLoggerBasic:
    """
    Testes básicos para get_logger() - a função principal.

    4 testes para cobrir casos básicos.
    """

    def test_get_logger_basic_creation(self, configured_logging):
        """
        Test 1/10: Criação básica de logger.

        Verifica que get_logger() retorna algo que pode logar.
        structlog pode usar LazyProxy, então testamos funcionalidade, não tipo.
        """
        # Obter logger básico
        logger = get_logger()

        # Verificar que tem métodos de logging
        assert hasattr(logger, "info")
        assert hasattr(logger, "error")
        assert hasattr(logger, "debug")
        assert callable(logger.info)

        # Testar que realmente funciona
        captured_output = StringIO()
        handler = logging.StreamHandler(captured_output)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logging.root.addHandler(handler)

        try:
            # Usar o logger
            logger.info("test_basic_logger", value=123)
            handler.flush()

            output = captured_output.getvalue().strip()
            assert output, "Logger should produce output"

            # Verificar que é JSON válido
            try:
                parsed = json.loads(output)
                assert parsed["event"] == "test_basic_logger"
                assert parsed["value"] == 123
            except json.JSONDecodeError:
                pytest.fail(f"Output is not valid JSON: {output}")

        finally:
            logging.root.removeHandler(handler)

    def test_get_logger_with_name(self, configured_logging):
        """
        Test 2/10: Logger com nome de módulo.

        Verifica que o nome do módulo é incluído no contexto.
        """
        module_name = "my_module.submodule"
        logger, output = capture_log_output(get_logger, module_name)

        # Verificar que output não está vazio
        assert output, "No log output produced"

        # Parsear JSON e verificar campo 'module'
        try:
            parsed = json.loads(output)
            assert "module" in parsed
            assert parsed["module"] == module_name
            assert parsed["event"] == "test_event"
        except json.JSONDecodeError:
            pytest.fail(f"Output is not valid JSON: {output}")

    def test_get_logger_with_layer_and_component(self, configured_logging):
        """
        Test 3/10: Logger com layer e component.

        Verifica que contexto arquitetural é adicionado.
        """
        logger, output = capture_log_output(
            get_logger,
            "test_module",
            layer="ingestion",
            component="ccxt-adapter",
            exchange="binance",
        )

        assert output, "No log output produced"

        try:
            parsed = json.loads(output)
            # Campos obrigatórios
            assert parsed["layer"] == "ingestion"
            assert parsed["component"] == "ccxt-adapter"
            assert parsed["exchange"] == "binance"
            assert parsed["module"] == "test_module"
        except json.JSONDecodeError:
            pytest.fail(f"Output is not valid JSON: {output}")

    def test_get_logger_with_initial_context(self, configured_logging):
        """
        Test 4/10: Logger com contexto inicial.

        Verifica que contextos adicionais são incluídos.
        """
        initial_context = {
            "service": "data-fetcher",
            "version": "1.2.3",
            "environment": "staging",
        }

        logger, output = capture_log_output(
            get_logger, "service_module", **initial_context
        )

        assert output, "No log output produced"

        try:
            parsed = json.loads(output)
            # Todos os contextos devem estar presentes
            for key, value in initial_context.items():
                assert parsed[key] == value
        except json.JSONDecodeError:
            pytest.fail(f"Output is not valid JSON: {output}")


class TestLayerSpecificLoggers:
    """
    Testes para fábricas específicas por camada.

    5 testes para cada fábrica principal.
    """

    def test_get_infrastructure_logger(self, configured_logging):
        """
        Test 5/10: Logger para camada de infraestrutura.

        Verifica que:
        - layer = "infrastructure"
        - component é configurável
        - Contexto adicional funciona
        """
        logger, output = capture_log_output(
            get_infrastructure_logger,
            "database-adapter",
            table="ohlcv",
            operation="batch_insert",
        )

        assert output, "No log output produced"

        try:
            parsed = json.loads(output)
            assert parsed["layer"] == "infrastructure"
            assert parsed["component"] == "database-adapter"
            assert parsed["table"] == "ohlcv"
            assert parsed["operation"] == "batch_insert"
        except json.JSONDecodeError:
            pytest.fail(f"Output is not valid JSON: {output}")

    def test_get_ingestion_logger_with_exchange(self, configured_logging):
        """
        Test 6/10: Logger para camada de ingestão com exchange.

        Caso especial: ingestion logger aceita parâmetro 'exchange'.
        """
        logger, output = capture_log_output(
            get_ingestion_logger,
            "ccxt-adapter",
            exchange="binance",
            symbol="BTC/USDT",
            market_type="linear_perpetual",
        )

        assert output, "No log output produced"

        try:
            parsed = json.loads(output)
            assert parsed["layer"] == "ingestion"
            assert parsed["component"] == "ccxt-adapter"
            assert parsed["exchange"] == "binance"
            assert parsed["symbol"] == "BTC/USDT"
            assert parsed["market_type"] == "linear_perpetual"
        except json.JSONDecodeError:
            pytest.fail(f"Output is not valid JSON: {output}")

    def test_get_ingestion_logger_without_exchange(self, configured_logging):
        """
        Test 7/10: Logger para ingestão sem exchange.

        Verifica que funciona mesmo sem parâmetro exchange.
        """
        logger, output = capture_log_output(
            get_ingestion_logger, "coinalyze-adapter", symbol="BTC", timeframe="1h"
        )

        assert output, "No log output produced"

        try:
            parsed = json.loads(output)
            assert parsed["layer"] == "ingestion"
            assert parsed["component"] == "coinalyze-adapter"
            assert parsed["symbol"] == "BTC"
            assert parsed["timeframe"] == "1h"
            # exchange não deve estar presente
            assert "exchange" not in parsed
        except json.JSONDecodeError:
            pytest.fail(f"Output is not valid JSON: {output}")

    def test_get_orchestration_logger(self, configured_logging):
        """
        Test 8/10: Logger para camada de orquestração.

        Verifica suporte para DAGs e tasks do Airflow.
        """
        logger, output = capture_log_output(
            get_orchestration_logger,
            component="airflow-task",
            dag_id="coinalyze_backfill",
            task_id="fetch_ohlcv",
            execution_date="2024-01-01",
        )

        assert output, "No log output produced"

        try:
            parsed = json.loads(output)
            assert parsed["layer"] == "orchestration"
            assert parsed["component"] == "airflow-task"
            assert parsed["dag_id"] == "coinalyze_backfill"
            assert parsed["task_id"] == "fetch_ohlcv"
            assert parsed["execution_date"] == "2024-01-01"
        except json.JSONDecodeError:
            pytest.fail(f"Output is not valid JSON: {output}")

    def test_get_transformation_logger(self, configured_logging):
        """
        Test 9/10: Logger para camada de transformação.
        """
        logger, output = capture_log_output(
            get_transformation_logger,
            "data-validator",
            validation_type="schema",
            ruleset="ohlcv_v1",
        )

        assert output, "No log output produced"

        try:
            parsed = json.loads(output)
            assert parsed["layer"] == "transformation"
            assert parsed["component"] == "data-validator"
            assert parsed["validation_type"] == "schema"
            assert parsed["ruleset"] == "ohlcv_v1"
        except json.JSONDecodeError:
            pytest.fail(f"Output is not valid JSON: {output}")

    def test_get_storage_logger(self, configured_logging):
        """
        Test 10/10: Logger para camada de storage.
        """
        logger, output = capture_log_output(
            get_storage_logger,
            "timescale-writer",
            table="open_interest",
            batch_size=1000,
        )

        assert output, "No log output produced"

        try:
            parsed = json.loads(output)
            assert parsed["layer"] == "storage"
            assert parsed["component"] == "timescale-writer"
            assert parsed["table"] == "open_interest"
            assert parsed["batch_size"] == 1000
        except json.JSONDecodeError:
            pytest.fail(f"Output is not valid JSON: {output}")


class TestLoggerAliasesAndBehavior:
    """
    Testes adicionais para aliases e comportamentos.

    Estes são extras além dos 10 testes planejados,
    mas importantes para completude.
    """

    def test_get_database_logger_alias(self, configured_logging):
        """
        Test extra: Alias get_database_logger.

        Verifica que é um alias para get_infrastructure_logger.
        """
        logger1, output1 = capture_log_output(get_database_logger, table="ohlcv")

        logger2, output2 = capture_log_output(
            get_infrastructure_logger, "database-adapter", table="ohlcv"
        )

        # Ambos devem produzir output
        assert output1 and output2

        try:
            parsed1 = json.loads(output1)
            parsed2 = json.loads(output2)

            # Devem ter a mesma estrutura básica
            assert parsed1["layer"] == "infrastructure"
            assert parsed2["layer"] == "infrastructure"
            assert parsed1["table"] == "ohlcv"
            assert parsed2["table"] == "ohlcv"

            # database logger deve ter component fixo
            assert parsed1["component"] == "database-adapter"

        except json.JSONDecodeError:
            pytest.fail("Output is not valid JSON")

    def test_get_processing_logger_alias(self, configured_logging):
        """
        Test extra: Alias get_processing_logger.

        Verifica backward compatibility.
        """
        logger, output = capture_log_output(
            get_processing_logger, "normalizer", data_type="ohlcv"
        )

        assert output, "No log output produced"

        try:
            parsed = json.loads(output)
            # processing_logger é alias para transformation_logger
            assert parsed["layer"] == "transformation"
            assert parsed["component"] == "normalizer"
            assert parsed["data_type"] == "ohlcv"
        except json.JSONDecodeError:
            pytest.fail(f"Output is not valid JSON: {output}")

    def test_logger_context_isolation(self, configured_logging):
        """
        Test extra: Isolamento de contexto entre loggers.

        Verifica que bind() em um logger não afeta outros.
        """
        # Criar dois loggers
        logger1 = get_logger("logger1")
        logger2 = get_logger("logger2")

        # Adicionar contexto apenas ao logger1
        logger1 = logger1.bind(service="service1", id=1)

        # Capturar output de ambos
        captured = StringIO()
        handler = logging.StreamHandler(captured)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logging.root.addHandler(handler)

        try:
            # Logar com logger1
            logger1.info("event1")

            # Logar com logger2
            logger2.info("event2", id=2)

            handler.flush()
            output = captured.getvalue().strip()
            lines = [l.strip() for l in output.split("\n") if l.strip()]

            # Devemos ter 2 linhas
            assert len(lines) == 2, f"Expected 2 lines, got {len(lines)}: {lines}"

            # Parsear e verificar isolamento
            parsed1 = json.loads(lines[0])
            parsed2 = json.loads(lines[1])

            # logger1 deve ter seu contexto
            if "event1" in parsed1.values():
                assert parsed1.get("service") == "service1"
                assert parsed1.get("id") == 1
            else:
                # Pode ser a segunda linha
                assert parsed2.get("service") == "service1"
                assert parsed2.get("id") == 1

            # logger2 não deve ter service, mas tem id=2 do log
            if "event2" in parsed1.values():
                assert "service" not in parsed1
                assert parsed1.get("id") == 2
            else:
                assert "service" not in parsed2
                assert parsed2.get("id") == 2

        finally:
            logging.root.removeHandler(handler)

    def test_logger_can_be_rebound(self, configured_logging):
        """
        Test extra: Rebinding de contexto.

        Verifica que podemos adicionar contexto incrementalmente.
        """
        logger = get_logger("rebind_test")

        # Primeiro bind
        logger = logger.bind(phase="startup", attempt=1)

        captured = StringIO()
        handler = logging.StreamHandler(captured)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logging.root.addHandler(handler)

        try:
            # Primeiro log
            logger.info("phase1")

            # Rebind com mais contexto
            logger = logger.bind(phase="processing", attempt=2, items=100)
            logger.info("phase2")

            # Rebind novamente
            logger = logger.bind(phase="cleanup", attempt=3, items=0)
            logger.info("phase3")

            handler.flush()
            output = captured.getvalue().strip()
            lines = [l.strip() for l in output.split("\n") if l.strip()]

            assert len(lines) == 3, f"Expected 3 lines, got {len(lines)}"

            # Verificar cada fase tem seu contexto
            phases = []
            for line in lines:
                parsed = json.loads(line)
                phases.append(parsed.get("phase"))

                if parsed["event"] == "phase1":
                    assert parsed["attempt"] == 1
                    assert "items" not in parsed
                elif parsed["event"] == "phase2":
                    assert parsed["attempt"] == 2
                    assert parsed["items"] == 100
                elif parsed["event"] == "phase3":
                    assert parsed["attempt"] == 3
                    assert parsed["items"] == 0

            assert set(phases) == {"startup", "processing", "cleanup"}

        finally:
            logging.root.removeHandler(handler)


# Para executar apenas estes testes:
# pytest tests/fixtures/test_observability/test_logger_factories.py -v
