"""Classify approval reviewers from session metadata, never transcript text."""


def approval_review_sql(metadata: str) -> str:
    # Older imports may contain malformed JSON. Unknown metadata stays visible.
    return f"""CASE WHEN json_valid({metadata}) THEN COALESCE(
        json_extract({metadata}, '$.thread_source') = 'guardian_review'
        OR json_extract({metadata}, '$.source.subagent.other') = 'guardian', 0)
        ELSE 0 END"""


HIDE_APPROVAL_REVIEWS_SQL = """s.id NOT IN (
    SELECT id FROM session_browse WHERE is_approval_review = 1
)"""
