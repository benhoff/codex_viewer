from __future__ import annotations

import unittest

from agent_operations_viewer.markdown_utils import render_markdown


class MarkdownUtilsTests(unittest.TestCase):
    def test_render_pipe_table(self) -> None:
        html = str(
            render_markdown(
                """
Intro

| Command | Score | Status |
| --- | ---: | :---: |
| `doctor` | 10 | **ok** |
| [docs](https://example.com) | 2 | review |
"""
            )
        )

        self.assertIn("<p>Intro</p>", html)
        self.assertIn('<div class="markdown-table-wrapper"><table>', html)
        self.assertIn("<th>Command</th>", html)
        self.assertIn('<th data-align="right">Score</th>', html)
        self.assertIn('<th data-align="center">Status</th>', html)
        self.assertIn("<td><code>doctor</code></td>", html)
        self.assertIn('<td data-align="right">10</td>', html)
        self.assertIn('<td data-align="center"><strong>ok</strong></td>', html)
        self.assertNotIn("| --- |", html)

    def test_render_pipe_table_preserves_code_and_escaped_pipes(self) -> None:
        html = str(
            render_markdown(
                r"""
| Pattern | Meaning |
| --- | --- |
| `a | b` | left \| right |
"""
            )
        )

        self.assertIn("<td><code>a | b</code></td>", html)
        self.assertIn("<td>left | right</td>", html)

    def test_pipe_text_without_delimiter_stays_paragraph(self) -> None:
        html = str(render_markdown("This has A | B\nbut no delimiter row."))

        self.assertEqual(html, "<p>This has A | B<br>\nbut no delimiter row.</p>")


if __name__ == "__main__":
    unittest.main()
