#!/bin/bash
# Mock agy для интеграционных тестов.
# Поведение управляется переменной среды MOCK_AGY_BEHAVIOR:
#   success    — возвращает ответ на stdout
#   ratelimit  — возвращает пустой stdout (эмулирует rate limit)
#   error      — exit code 1
#   slow       — ждёт 10 секунд (для тестов таймаута)

BEHAVIOR="${MOCK_AGY_BEHAVIOR:-success}"

case "$BEHAVIOR" in
  success)
    echo "Mock response to: $*"
    exit 0
    ;;
  ratelimit)
    # Пустой stdout, exit 0 — как настоящий agy при rate limit.
    exit 0
    ;;
  error)
    echo "Error occurred" >&2
    exit 1
    ;;
  slow)
    sleep 10
    echo "Slow response"
    exit 0
    ;;
  *)
    echo "Unknown behavior: $BEHAVIOR" >&2
    exit 1
    ;;
esac
