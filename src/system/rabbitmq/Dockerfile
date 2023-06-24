FROM rabbitmq:3.11.13-management-alpine
USER rabbitmq
HEALTHCHECK --interval=1s --timeout=3s --retries=50 CMD rabbitmq-diagnostics check_port_connectivity
