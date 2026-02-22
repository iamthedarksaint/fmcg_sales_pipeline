from airflow.utils.email import send_email
from airflow.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log

def on_failure_alert(context):
    """Send email alert on task failure."""
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    log_url = context["task_instance"].log_url
    exception = context.get("exception", "Unknown error")

    subject = f"[FMCG Pipeline] FAILED: {dag_id}.{task_id}"
    body = f"""
    <h3>Pipeline Failure Alert</h3>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Error:</b> {exception}</p>
    <p><b>Log:</b> <a href="{log_url}">{log_url}</a></p>
    """
    try:
        send_email(to="bojzino128.com", subject=subject, html_content=body)
    except Exception as e:
        log.warning(f"Could not send alert email: {e}")