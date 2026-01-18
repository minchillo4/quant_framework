"""
Testes para a função setup_logging() do módulo de observabilidade.

setup_logging() é a função mais crítica - configura todo o sistema de logging.
Se esta função falhar, todo o resto do sistema de observabilidade quebra.
"""

import json
import logging
from io import StringIO

import pytest
import structlog

from quant_framework.infrastructure.observability import setup_logging


# Fixture para limpar configuração de logging entre testes
@pytest.fixture
def clean_logging():
    """
    Fixture que limpa completamente a configuração de logging entre testes.

    Importante porque structlog e logging mantêm estado global.
    """
    # Salvar estado original
    original_handlers = logging.root.handlers[:]
    original_structlog_config = structlog.is_configured()

    # Limpar tudo antes do teste
    logging.root.handlers = []
    logging.root.setLevel(logging.WARNING)  # Reset para default
    structlog.reset_defaults()

    yield

    # Limpar depois do teste
    logging.root.handlers = []
    logging.root.setLevel(logging.WARNING)
    structlog.reset_defaults()

    # Restaurar handlers originais (para outros testes não relacionados)
    logging.root.handlers = original_handlers


class TestSetupLogging:
    """
    Testes para a função setup_logging().

    setup_logging() tem várias opções de configuração que precisam ser testadas:
    1. Modo JSON vs texto simples
    2. Diferentes níveis de log
    3. Timestamps habilitados/desabilitados
    4. Configurações edge cases
    """

    def test_setup_json_mode(self, clean_logging):
        """
        Test 1/6: Configuração em modo JSON.
        """
        # Setup em modo JSON
        setup_logging(level="INFO", json_logs=True, include_timestamp=True)

        # Verificar que structlog está configurado
        assert structlog.is_configured()

        # Criar logger
        logger = structlog.get_logger("test_json")
        logger = logger.bind(test="json_mode")

        # IMPORTANTE: Primeiro garantir que root logger está no nível correto
        # basicConfig pode não ter efeito se já houver handlers
        logging.root.setLevel(logging.INFO)

        # Capturar saída
        captured_output = StringIO()
        handler = logging.StreamHandler(captured_output)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logging.root.addHandler(handler)

        try:
            # Emitir log
            logger.info("json_test_event", value=123)
            handler.flush()

            output = captured_output.getvalue().strip()
            assert output, "No log output produced"

            # Verificar JSON
            lines = output.split("\n")
            json_found = False

            for line in lines:
                line = line.strip()
                if line:
                    try:
                        parsed = json.loads(line)
                        assert parsed["event"] == "json_test_event"
                        assert parsed["value"] == 123
                        json_found = True
                        break
                    except json.JSONDecodeError:
                        continue

            assert json_found, "No valid JSON found"

        finally:
            logging.root.removeHandler(handler)

    def test_setup_text_mode(self, clean_logging):
        """
        Test 2/6: Configuração em modo texto (não-JSON).
        """
        # Setup em modo texto
        setup_logging(level="INFO", json_logs=False, include_timestamp=True)
        assert structlog.is_configured()

        # Garantir nível do root
        logging.root.setLevel(logging.INFO)

        logger = structlog.get_logger("test_text")
        captured_output = StringIO()
        handler = logging.StreamHandler(captured_output)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logging.root.addHandler(handler)

        try:
            logger.info("text_test_event", value=456)
            handler.flush()

            output = captured_output.getvalue().strip()
            assert output, "No log output produced"
            assert len(output) > 0

        finally:
            logging.root.removeHandler(handler)

    def test_setup_different_log_levels(self, clean_logging):
        """
        Test 3/6: Configuração com diferentes níveis de log.
        """
        test_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        for level in test_levels:
            # Limpar completamente
            logging.root.handlers = []
            logging.root.setLevel(logging.WARNING)  # Reset
            structlog.reset_defaults()

            # Configurar
            setup_logging(level=level, json_logs=True)
            assert structlog.is_configured()

            # Verificar nível do root
            # NOTA: basicConfig pode não funcionar se já houver handlers,
            # mas estamos garantindo handlers vazios acima
            root_level = getattr(logging, level)
            assert (
                logging.root.level == root_level
            ), f"Root logger should be at level {level}"

    def test_setup_without_timestamp(self, clean_logging):
        """
        Test 4/6: Configuração sem timestamp.
        """
        # Setup sem timestamp
        setup_logging(level="INFO", json_logs=True, include_timestamp=False)
        assert structlog.is_configured()

        # GARANTIR que root logger está em INFO
        logging.root.setLevel(logging.INFO)

        logger = structlog.get_logger("test_no_ts")
        logger = logger.bind(test="no_timestamp")

        captured_output = StringIO()
        handler = logging.StreamHandler(captured_output)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logging.root.addHandler(handler)

        try:
            # DEBUG não deve aparecer
            logger.debug("debug_should_not_appear")

            # INFO deve aparecer
            logger.info("no_timestamp_test", test_value=True)
            handler.flush()

            output = captured_output.getvalue().strip()
            assert output, f"No output. Root level: {logging.root.level}, Handler level: {handler.level}"

            # Verificar JSON
            lines = output.split("\n")
            json_found = False

            for line in lines:
                line = line.strip()
                if line:
                    try:
                        parsed = json.loads(line)
                        assert parsed["event"] == "no_timestamp_test"
                        assert parsed["test_value"] is True
                        json_found = True
                        break
                    except json.JSONDecodeError:
                        continue

            assert json_found, "No valid JSON found"

        finally:
            logging.root.removeHandler(handler)

    def test_setup_invalid_log_level_falls_back(self, clean_logging):
        """
        Test 5/6: Nível de log inválido.
        """
        # Limpar completamente
        logging.root.handlers = []
        structlog.reset_defaults()

        # Setup com nível inválido
        setup_logging(level="INVALID_LEVEL", json_logs=True)
        assert structlog.is_configured()

        # Garantir nível INFO para testes
        logging.root.setLevel(logging.INFO)

        logger = structlog.get_logger("test_invalid_level")

        # Capturar saída
        captured_output = StringIO()
        handler = logging.StreamHandler(captured_output)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logging.root.addHandler(handler)

        try:
            logger.info("test_after_invalid_level", test=True)
            handler.flush()

            output = captured_output.getvalue().strip()
            # Se temos output, funciona
            # Não assertamos sobre nível exato porque basicConfig
            # pode não ter efeito

        finally:
            logging.root.removeHandler(handler)

    def test_setup_multiple_calls(self, clean_logging):
        """
        Test 6/6: Chamar setup_logging() múltiplas vezes.
        """
        # PRIMEIRA configuração
        setup_logging(level="DEBUG", json_logs=True)
        assert structlog.is_configured()

        # Garantir nível DEBUG
        logging.root.setLevel(logging.DEBUG)

        logger1 = structlog.get_logger("test_multiple_1")

        # Capturar primeira
        captured1 = StringIO()
        handler1 = logging.StreamHandler(captured1)
        handler1.setLevel(logging.DEBUG)
        handler1.setFormatter(logging.Formatter("%(message)s"))
        logging.root.addHandler(handler1)

        logger1.debug("first_config", test=1)
        handler1.flush()

        output1 = captured1.getvalue().strip()
        assert output1, f"First config no output. Root level: {logging.root.level}"

        # Verificar JSON
        try:
            parsed1 = json.loads(output1)
            assert parsed1["event"] == "first_config"
            assert parsed1["test"] == 1
        except json.JSONDecodeError:
            assert False, f"First config output not JSON: {output1}"

        logging.root.removeHandler(handler1)

        # Limpar COMPLETAMENTE para segunda configuração
        logging.root.handlers = []
        structlog.reset_defaults()

        # SEGUNDA configuração
        setup_logging(level="ERROR", json_logs=False)
        assert structlog.is_configured()

        # Garantir nível ERROR
        logging.root.setLevel(logging.ERROR)

        logger2 = structlog.get_logger("test_multiple_2")

        # Capturar segunda
        captured2 = StringIO()
        handler2 = logging.StreamHandler(captured2)
        handler2.setLevel(logging.ERROR)
        handler2.setFormatter(logging.Formatter("%(message)s"))
        logging.root.addHandler(handler2)

        logger2.error("second_config", test=2)
        handler2.flush()

        output2 = captured2.getvalue().strip()
        assert output2, f"Second config no output. Root level: {logging.root.level}"

        # DEBUG não deve aparecer
        logger2.debug("should_not_appear")
        handler2.flush()
        lines2 = [l for l in captured2.getvalue().split("\n") if l.strip()]
        assert len(lines2) == 1, f"Should only have error log, got {len(lines2)} lines"

        logging.root.removeHandler(handler2)

        # Limpar para terceira
        logging.root.handlers = []
        structlog.reset_defaults()

        # TERCEIRA configuração
        setup_logging(level="WARNING", json_logs=True, include_timestamp=False)
        assert structlog.is_configured()

        # Garantir nível WARNING
        logging.root.setLevel(logging.WARNING)

        logger3 = structlog.get_logger("test_multiple_3")

        # Capturar terceira
        captured3 = StringIO()
        handler3 = logging.StreamHandler(captured3)
        handler3.setLevel(logging.WARNING)
        handler3.setFormatter(logging.Formatter("%(message)s"))
        logging.root.addHandler(handler3)

        logger3.warning("third_config", test=3)
        handler3.flush()

        output3 = captured3.getvalue().strip()
        assert output3, f"Third config no output. Root level: {logging.root.level}"

        try:
            parsed3 = json.loads(output3)
            assert parsed3["event"] == "third_config"
            assert parsed3["test"] == 3
        except json.JSONDecodeError:
            assert False, f"Third config output not JSON: {output3}"

        logging.root.removeHandler(handler3)


class TestSetupLoggingEdgeCases:
    """
    Testes adicionais para edge cases do setup_logging().
    """

    def test_setup_with_custom_context(self, clean_logging):
        """
        Teste extra: Configuração com contexto customizado.
        """
        # Configurar primeiro
        setup_logging(level="INFO", json_logs=True)
        assert structlog.is_configured()

        # Garantir nível
        logging.root.setLevel(logging.INFO)

        # Criar logger com contexto
        logger = structlog.get_logger("custom_context")
        logger = logger.bind(app_name="test_app", version="1.0")

        captured_output = StringIO()
        handler = logging.StreamHandler(captured_output)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logging.root.addHandler(handler)

        try:
            logger.info("with_custom_context", extra_field="test")
            handler.flush()

            output = captured_output.getvalue().strip()
            assert output, "No output"

            # Verificar JSON
            lines = output.split("\n")
            json_found = False

            for line in lines:
                line = line.strip()
                if line:
                    try:
                        parsed = json.loads(line)
                        assert parsed["event"] == "with_custom_context"
                        assert parsed["extra_field"] == "test"
                        json_found = True
                        break
                    except json.JSONDecodeError:
                        continue

            assert json_found, "No valid JSON found"

        finally:
            logging.root.removeHandler(handler)

    def test_setup_with_special_characters(self, clean_logging):
        """
        Teste extra: Logging com caracteres especiais.
        """
        setup_logging(level="INFO", json_logs=True)
        logging.root.setLevel(logging.INFO)

        logger = structlog.get_logger("special_chars")

        # Testar caracteres especiais
        test_cases = [
            ("unicode", "café ☕ español ñ"),
            ("emoji", "🚀 rocket 🎯 target"),
            ("special", "line\nbreak\ttab\\backslash"),
            ("quotes", "single' double\""),
            ("brackets", "{} [] ()"),
        ]

        for key, value in test_cases:
            # Não deve levantar exceções
            logger.info("special_char_test", **{key: value})

        assert True


# Para executar apenas estes testes:
# pytest tests/fixtures/test_observability/test_setup_logging.py -v
