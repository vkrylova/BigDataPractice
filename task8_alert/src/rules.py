import pandas as pd


def rule_global_errors(history_df: pd.DataFrame) -> list[tuple[str, str]]:
    """
    Rule 1: Alert on > 10 errors in less than one minute.

    Args:
        history_df: DataFrame with error history.

    Returns:
        List of triggered alerts (message, alert_id).
    """

    alerts = []
    if history_df.empty:
        return alerts

    latest_time = history_df["timestamp"].max()
    one_minute_ago = latest_time - pd.Timedelta(minutes=1)

    recent_errors = history_df[history_df["timestamp"] >= one_minute_ago]

    if len(recent_errors) > 10:
        message = f"GLOBAL SPIKE: {len(recent_errors)} errors in the last minute!"
        alert_id = "GLOBAL_SPIKE_1MIN"
        alerts.append((message, alert_id))

    return alerts


def rule_bundle_errors(history_df: pd.DataFrame) -> list[tuple[str, str]]:
    """
    Rule 2: Alert on > 10 errors in less than one hour for a specific bundle.

    Args:
        history_df: DataFrame with error history.

    Returns:
        List of triggered alerts (message, alert_id).
    """

    alerts = []

    if history_df.empty:
        return alerts

    bundle_counts = history_df.groupby("bundle_id").size()
    violating_bundles = bundle_counts[bundle_counts > 10]

    for bundle, count in violating_bundles.items():
        message = f"Application:\n{bundle}\n{count} errors in 1 hour!"
        alert_id = f"BUNDLE_1H_{bundle}"
        alerts.append((message, alert_id))

    return alerts


# Add new rules here
ACTIVE_RULES = [
    rule_global_errors,
    rule_bundle_errors,
]
