config = {
    "aws_access_key": "/* REDACTED_BY_ENTERPRISE_COMPLIANCE_LAYER [AWS Access Key] */",
    "database_url": "postgresql://user:password@localhost:5432/mydb",
    "slack_webhook": "/* REDACTED_BY_ENTERPRISE_COMPLIANCE_LAYER [Slack Webhook URL] */",
}

def get_config_value(key):
    return config.get(key, None)