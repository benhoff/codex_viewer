"""Deterministic grading views. Original capture records are never modified."""
from collections import Counter
import hashlib
import re

VERSION = "evidence-consolidation-v1"
TOOL_KINDS = {"command", "tool_result"}

# Recognize timestamps, not arbitrary changing numbers in diagnostic messages.
# Process identities, levels, filenames, locations and measured values stay in body.
LOG = re.compile(r"^(?P<service>(?:[\w.-]+\s+\|\s*)?)(?P<level><[A-Z]>)?(?P<time>\[?"
                 r"\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?\]?)\s+(?P<body>.+)$")
SYSLOG = re.compile(r"^(?P<time>(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+(?P<body>\S+\s+[^:]+:\s+.+)$")
PIDSTAT = re.compile(r"^\s*\d{2}:\d{2}:\d{2}\s+\d+\s+\d+\s+(?:-?\d+(?:\.\d+)?\s+){4,}\S.*$")
OPTION_RANGE = re.compile(r"(?m)^([ \t]*\[?--[\w-]+\s+)\{(-?\d+(?:,-?\d+){99,})\}")
LINT = re.compile(r'^\s*(\d+:\d+)\s+(error|warning)\s+(.+?)\s{2,}([@\w./-]+)\s*$')


def digest(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def unwrap_header(text):
    """Only remove the known legacy exec envelope; retain command and exit code."""
    match = re.match(r"\A\s*(?P<command>Command: [^\n]*\n)?Chunk ID: [^\n]*\n"
                     r"Wall time: [^\n]*\nProcess exited with code (?P<exit>-?\d+)\n"
                     r"(?:Original token count: \d+\n)?Output:\n", text)
    if not match:
        return text
    return (match['command'] or '') + text[match.end():] + f"\nExit code: {match['exit']}"


def compact_options(text):
    if not re.search(r"(?m)^usage: \S", text):
        return text
    def replace(match):
        strings = match[2].split(',')
        # Avoid huge integer parsing or changing leading-zero formatting.
        if any(len(v) > 12 for v in strings):
            return match[0]
        values = [int(v) for v in strings]
        if any(str(v) != s for v, s in zip(values, strings)) or any(b != a + 1 for a, b in zip(values, values[1:])):
            return match[0]
        return f"{match[1]}{{integers {values[0]} through {values[-1]} inclusive; {len(values)} choices, consolidated}}"
    return OPTION_RANGE.sub(replace, text)


def compact_artifacts(text):
    lines = text.splitlines(keepends=True)
    output, artifact = [], None
    for number, line in enumerate(lines, 1):
        probe = line.replace('\\"', '"')
        kind = None
        if len(line) > 4096:
            if re.match(r'^[^\s"{}:]*\bsearchindex\.js:\d+:Search\.setIndex\(', probe):
                kind = 'generated documentation search index'
            elif (re.match(r'^[^\s"{}:]+:\d+:', probe) and (re.search(r"\.map:\d+:", probe) or '/pnpm/store/' in probe)
                  and re.match(r'^[^\s"{}:]+:\d+:\{\s*"version"\s*:\s*3\s*,', probe)
                  and ('"mappings"' in probe or (re.search(r'"sources"\s*:\s*\[', probe)
                                                           and re.search(r'"sourcesContent"\s*:\s*\[', probe)))):
                kind = 'generated source map'
            elif artifact == 'generated source map' and re.fullmatch(r'[A-Za-z0-9+/;,=]+(?:"\s*})?\s*', line):
                kind = artifact
            elif artifact == 'generated documentation search index' and re.match(r'^[A-Za-z0-9_\\-]+":\d+[,}]', line):
                kind = artifact
        if kind:
            artifact = kind
            # Preserve the origin from the recognized first line, not an arbitrary
            # content prefix. Continuations retain the same record/line provenance.
            path = re.match(r'([^\s"{}]{1,400}:\d+:)', line)
            origin = f" origin={path[1]}" if path else ''
            output.append(f"[Consolidated {kind}; line {number}; {len(line.encode())} bytes; sha256={digest(line)};{origin} original retained in event]\n")
        else:
            output.append(line)
            # Only importer truncation notices/blank lines may bridge segments.
            if line.strip() and not re.match(r'^(?:\.\.\.|Warning: truncated|\[Captured output)', line):
                artifact = None
    return ''.join(output)


def log_identity(line):
    match = LOG.match(line) or SYSLOG.match(line)
    if match:
        return (match.groupdict().get('service', ''), match.groupdict().get('level', ''), match['body'])
    return None


def compact_logs(text):
    """Compress only contiguous equal messages; never move a failure past recovery."""
    lines, output, index = text.splitlines(keepends=True), [], 0
    while index < len(lines):
        key = log_identity(lines[index].rstrip('\r\n'))
        end = index + 1
        if key is not None:
            while end < len(lines) and log_identity(lines[end].rstrip('\r\n')) == key:
                end += 1
        if end - index >= 4:
            block = ''.join(lines[index:end])
            replacement = (lines[index].rstrip('\r\n') + '\n'
                           f"[Consolidated {end-index} consecutive occurrences; source lines {index+1}-{end}; "
                           f"first and last samples retained; sha256={digest(block)}]\n" + lines[end-1])
            output.append(replacement if len(replacement) < len(block) else block)
        else:
            output.extend(lines[index:end])
        index = end
    return ''.join(output)


def request_view(event):
    """Remove only long runs of recognizable diagnostics from repeated context.

    Every unrecognized line (including prose inside pasted material) stays quoted.
    The complete user message remains primary evidence for extraction.
    """
    if event.get('role') != 'user' or event.get('kind') != 'message':
        return event
    lines, output, index, omitted = event['text'].splitlines(keepends=True), [], 0, 0
    def diagnostic(line):
        return bool(log_identity(line.rstrip('\r\n')) or PIDSTAT.match(line) or LINT.match(line))
    while index < len(lines):
        end = index
        while end < len(lines) and diagnostic(lines[end]):
            end += 1
        if end - index >= 5:
            output.append(f"[Diagnostic attachment: {end-index} lines at event {event['event_index']}, "
                          f"lines {index+1}-{end}; retained in primary evidence.]\n")
            omitted += end - index
            index = end
        else:
            output.append(lines[index])
            index += 1
    return {**event, 'text': ''.join(output), 'diagnostic_lines_in_evidence': omitted} if omitted else event


def compact_lint(text):
    """Keep every source location; group only adjacent identical ESLint diagnostics."""
    lines, output, index = text.splitlines(keepends=True), [], 0
    while index < len(lines):
        match = LINT.match(lines[index])
        end, locations = index + 1, [match[1]] if match else []
        while match and end < len(lines):
            following = LINT.match(lines[end])
            if not following or following.groups()[1:] != match.groups()[1:]:
                break
            locations.append(following[1])
            end += 1
        block = ''.join(lines[index:end])
        if len(locations) >= 4:
            summary = (f"[Consolidated {len(locations)} lint {match[2]} diagnostics; rule={match[4]}; "
                       f"message={match[3]}; all locations in original order={','.join(locations)}]\n")
            output.append(summary if len(summary) < len(block) else block)
        else:
            output.append(block)
        index = end
    return ''.join(output)


def consolidate_events(events, *, criteria=''):
    result, seen, changes = [], {}, []
    # A named artifact may itself be the subject of the review. Favor retaining
    # it whenever the user's request or acceptance criteria explicitly names it.
    artifact_task = any(re.search(r'\b(?:source[- ]?maps?|searchindex\.js|search index|package cache|pnpm store)\b', text, re.I)
                        for text in [criteria] + [e.get('text', '') for e in events if e.get('role') == 'user'])
    for event in events:
        original = event.get('text', '')
        text, rules = original, []
        # Patches, instructions, assistant deliverables, and user requests are
        # never rewritten by the tool-output rules.
        patch = re.search(r'(?m)^(?:diff --git |@@ |\*\*\* (?:Begin Patch|Update File:|Add File:))', original)
        if event.get('kind') in TOOL_KINDS and event.get('role') not in {'user', 'assistant', 'system', 'developer'} and not patch:
            for name, transform in [('legacy_transport', unwrap_header), ('integer_choices', compact_options),
                                    ('generated_artifact', compact_artifacts), ('repeated_log', compact_logs),
                                    ('lint_diagnostics', compact_lint)]:
                if name == 'generated_artifact' and artifact_task:
                    continue
                updated = transform(text)
                if updated != text and len(updated.encode()) < len(text.encode()):
                    text, rules = updated, rules + [name]
            identity = (event.get('kind'), event.get('tool_name'), event.get('exit_code'), text, event.get('command'))
            # Only exact full outputs with identical status. Keep each occurrence
            # as an indexed event, so repetition/failure history is not erased.
            if len(text) > 1000 and identity in seen:
                previous = seen[identity]
                text = (f"[Repeated output: identical to event {previous}; this occurrence and its exit status are retained. "
                        f"Source sha256={digest(original)}]\nFirst sample: {text[:160]}\nLast sample: {text[-160:]}")
                rules.append('exact_repeat')
            else:
                seen[identity] = event['event_index']
        if rules and len(original.encode()) - len(text.encode()) > 220:
            change = {'event_index': event['event_index'], 'rules': rules,
                      'original_bytes': len(original.encode()), 'retained_bytes': len(text.encode()), 'sha256': digest(original)}
            changes.append(change)
            event = {**event, 'text': text, 'consolidation': {'rules': rules, 'original_bytes': change['original_bytes'], 'sha256': change['sha256']}}
        result.append(event)
    return result, {'version': VERSION, 'changed_events': len(changes),
                    'bytes_removed': sum(c['original_bytes'] - c['retained_bytes'] for c in changes),
                    'rules': dict(Counter(rule for c in changes for rule in c['rules'])), 'events': changes}
