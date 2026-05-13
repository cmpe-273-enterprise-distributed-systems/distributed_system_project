---
name: summarize-long-text-bullets
description: Summarize long text into 3-5 concise bullet points.
---

# Summarize Long Text into Concise Bullet Points

## Overview

This skill teaches an AI agent how to compress long text into 3–5 high-value bullet points. The summary should preserve the main topic, key facts, and outcome while removing repetition, examples, and filler. The output must look like a direct bullet list, not a narrated response.

## Core Rules

1. **Output only bullets**  
   Do not add introductions like “Here is a summary” or “Below are the points.”

2. **Use the bullet character `•`**  
   Each line should begin with `•` unless the user explicitly requests a different format.

3. **Keep 3–5 bullets**  
   Fewer than 3 may miss key ideas; more than 5 may become too detailed.

4. **Preserve important terms and names**  
   If the source text contains a central topic name, such as “Industrial Revolution,” include it in the summary.

5. **Prefer concise, complete statements**  
   Each bullet should capture one major idea in plain language.

## Example Patterns

### Example 1
Input: a historical article about industrialization

```markdown
• The Industrial Revolution was a major period of industrialization and innovation in the late 1700s and early 1800s.
• It began in Great Britain before spreading to other parts of the world.
• The Second Industrial Revolution introduced mechanization in agriculture and textile manufacturing.
• Steam ships and railroads transformed transportation and reshaped society and the economy.
```

### Example 2
Input: a product update document

```markdown
• The new release adds faster search and improved filtering.
• Several bugs were fixed in login and payment flows.
• Mobile performance and stability were significantly improved.
• The team plans a follow-up update next month.
```

### Example 3
Input: a meeting transcript

```markdown
• The team agreed to prioritize the mobile redesign.
• Engineering will address performance issues before launch.
• Marketing will deliver updated copy by Friday.
• The next review meeting is scheduled for Tuesday.
```

## Best Practices

- Scan for repeated ideas and merge them into one bullet.
- Keep the summary faithful to the source; do not invent details.
- Retain critical dates, numbers, entities, and conclusions.
- Avoid weak phrasing such as “This article talks about.”
- If the user asks for “bullet points,” ensure the output visibly uses bullets, not prose paragraphs.