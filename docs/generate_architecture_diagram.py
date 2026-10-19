"""Generate docs/architecture.svg for the Enterprise End-to-End Data
Pipeline portfolio project. Pure stdlib, no deps.

Usage:
    python docs/generate_architecture_diagram.py
    # then rasterize to PNG, e.g.:
    npx -y resvg-cli --fit-zoom 2.4 docs/architecture.svg docs/architecture.png
"""

FONT = "Liberation Sans, DejaVu Sans, sans-serif"

W, H = 1700, 1230

parts = []


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def box(x, y, w, h, title, lines=None, fill="#eff6ff", stroke="#2563eb",
        title_color="#0f172a", text_color="#334155", stroke_width=2,
        title_size=15, text_size=12.5, rx=10, dashed=False):
    lines = lines or []
    dash = ' stroke-dasharray="6,4"' if dashed else ""
    parts.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="{stroke_width}"{dash}/>'
    )
    n_lines = len(lines)
    block_h = 20 + n_lines * 17
    ty = y + h / 2 - block_h / 2 + 16
    parts.append(
        f'<text x="{x + w/2:.1f}" y="{ty:.1f}" font-family="{FONT}" '
        f'font-size="{title_size}" font-weight="700" fill="{title_color}" '
        f'text-anchor="middle">{esc(title)}</text>'
    )
    ty += 22
    for line in lines:
        parts.append(
            f'<text x="{x + w/2:.1f}" y="{ty:.1f}" font-family="{FONT}" '
            f'font-size="{text_size}" fill="{text_color}" '
            f'text-anchor="middle">{esc(line)}</text>'
        )
        ty += 17
    return (x, y, w, h)


def section_label(x, y, text, color="#0f172a", size=19):
    parts.append(
        f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" '
        f'font-weight="700" fill="{color}">{esc(text)}</text>'
    )


def center_bottom(b, frac=0.5):
    x, y, w, h = b
    return (x + w * frac, y + h)


def center_top(b, frac=0.5):
    x, y, w, h = b
    return (x + w * frac, y)


def arrow(p1, p2, color="#334155", width=2.2, dashed=False, label=None,
          label_size=11.5, marker="arrowhead"):
    x1, y1 = p1
    x2, y2 = p2
    dash = ' stroke-dasharray="7,5"' if dashed else ""
    if abs(x1 - x2) < 1 or abs(y1 - y2) < 1:
        path = f"M {x1:.1f} {y1:.1f} L {x2:.1f} {y2:.1f}"
    else:
        midy = (y1 + y2) / 2
        path = f"M {x1:.1f} {y1:.1f} L {x1:.1f} {midy:.1f} L {x2:.1f} {midy:.1f} L {x2:.1f} {y2:.1f}"
    parts.append(
        f'<path d="{path}" fill="none" stroke="{color}" stroke-width="{width}"{dash} '
        f'marker-end="url(#{marker})"/>'
    )
    if label:
        lx, ly = (x1 + x2) / 2, (y1 + y2) / 2
        w_est = len(label) * 6.3 + 8
        parts.append(f'<rect x="{lx - w_est/2:.1f}" y="{ly-21:.1f}" width="{w_est:.1f}" height="16" fill="#f8fafc"/>')
        parts.append(
            f'<text x="{lx:.1f}" y="{ly-9:.1f}" font-family="{FONT}" font-size="{label_size}" '
            f'fill="#475569" text-anchor="middle">{esc(label)}</text>'
        )


def multi_arrow(points, color="#334155", width=2.2, dashed=False, label=None,
                 label_size=11.5, marker="arrowhead", label_seg=0):
    """Polyline through explicit waypoints, arrowhead at the end. label_seg
    picks which segment (0-indexed) the label sits above."""
    dash = ' stroke-dasharray="7,5"' if dashed else ""
    path = "M " + " L ".join(f"{x:.1f} {y:.1f}" for x, y in points)
    parts.append(
        f'<path d="{path}" fill="none" stroke="{color}" stroke-width="{width}"{dash} '
        f'marker-end="url(#{marker})"/>'
    )
    if label:
        (x1, y1), (x2, y2) = points[label_seg], points[label_seg + 1]
        lx, ly = (x1 + x2) / 2, (y1 + y2) / 2
        w_est = len(label) * 6.3 + 8
        parts.append(f'<rect x="{lx - w_est/2:.1f}" y="{ly-21:.1f}" width="{w_est:.1f}" height="16" fill="#f8fafc"/>')
        parts.append(
            f'<text x="{lx:.1f}" y="{ly-9:.1f}" font-family="{FONT}" font-size="{label_size}" '
            f'fill="#475569" text-anchor="middle">{esc(label)}</text>'
        )


# ---------------------------------------------------------------------------
defs = f'''<defs>
  <marker id="arrowhead" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto">
    <path d="M0,0 L8,3 L0,6 Z" fill="#334155"/>
  </marker>
  <marker id="arrowhead_teal" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto">
    <path d="M0,0 L8,3 L0,6 Z" fill="#0f766e"/>
  </marker>
</defs>'''

parts.append(f'<rect x="0" y="0" width="{W}" height="{H}" fill="#f8fafc"/>')

parts.append(
    f'<text x="{W/2}" y="42" font-family="{FONT}" font-size="28" font-weight="700" '
    f'fill="#0f172a" text-anchor="middle">Enterprise End-to-End Data Pipeline</text>'
)
parts.append(
    f'<text x="{W/2}" y="66" font-family="{FONT}" font-size="14.5" '
    f'fill="#64748b" text-anchor="middle">Retail orders portfolio project '
    f'&#8212; batch, streaming, and governance pipelines on Snowflake + AWS</text>'
)

# Legend (top-right, fits in the gap between subtitle and the content row,
# starts after the "Pipeline B" section label ends around x1290)
lx, ly = 1340, 72
parts.append(f'<rect x="{lx}" y="{ly}" width="300" height="52" rx="8" fill="#ffffff" stroke="#e2e8f0" stroke-width="1.5"/>')
parts.append(f'<line x1="{lx+12}" y1="{ly+16}" x2="{lx+38}" y2="{ly+16}" stroke="#334155" stroke-width="2.2" marker-end="url(#arrowhead)"/>')
parts.append(f'<text x="{lx+45}" y="{ly+20}" font-family="{FONT}" font-size="11" fill="#475569">primary data flow</text>')
parts.append(f'<line x1="{lx+12}" y1="{ly+36}" x2="{lx+38}" y2="{ly+36}" stroke="#334155" stroke-width="2.2" stroke-dasharray="7,5" marker-end="url(#arrowhead)"/>')
parts.append(f'<text x="{lx+45}" y="{ly+40}" font-family="{FONT}" font-size="11" fill="#475569">alternative / side path</text>')

# ---------------------------------------------------------------------------
# Pipeline A (x90-760)
# ---------------------------------------------------------------------------
section_label(90, 112, "Pipeline A — Batch ingestion", color="#1d4ed8")

files = box(90, 130, 300, 60, "CSV / JSON files",
            ["customers, orders"], fill="#eff6ff", stroke="#93c5fd")

s3 = box(90, 225, 300, 65, "S3  incoming/",
         ["orders/, customers/ prefixes"], fill="#dbeafe", stroke="#2563eb")

lambda_box = box(90, 330, 320, 90, "Lambda: file_tracker",
                  ["ETag dedup + validation,", "quarantines invalid files"],
                  fill="#eff6ff", stroke="#2563eb")

snowpipe = box(440, 330, 320, 70, "Snowpipe",
                ["auto-ingest, SQS-triggered"], fill="#dbeafe", stroke="#2563eb")

dynamo = box(90, 450, 150, 60, "DynamoDB",
             ["FileIngestionTracker"], fill="#f8fafc", stroke="#94a3b8",
             title_size=13, text_size=11)

quarantine = box(260, 450, 150, 60, "S3  quarantine/",
                  ["invalid files"], fill="#fef2f2", stroke="#fca5a5",
                  title_size=13, text_size=11)

arrow(center_bottom(files), center_top(s3))
arrow(center_bottom(s3, 0.25), center_top(lambda_box, 0.3), label="ObjectCreated")
arrow(center_bottom(s3, 0.75), center_top(snowpipe, 0.5), label="ObjectCreated")
arrow(center_bottom(lambda_box, 0.25), center_top(dynamo, 0.5))
arrow(center_bottom(lambda_box, 0.75), center_top(quarantine, 0.5))

# ---------------------------------------------------------------------------
# Pipeline B (x860-1610)
# ---------------------------------------------------------------------------
section_label(860, 112, "Pipeline B — Real-time event processing", color="#6d28d9")

producer = box(860, 130, 750, 60, "event_producer.py",
                ["simulated purchase / payment events"],
                fill="#f5f3ff", stroke="#c4b5fd")

kafka = box(860, 225, 750, 65, "Kafka  order_events topic",
            ["local via docker-compose (Kinesis-swappable)"],
            fill="#ede9fe", stroke="#7c3aed")

spark = box(860, 330, 750, 90, "Spark Structured Streaming",
            ["checkpointed, 10-min watermark,", "dedup by event_id"],
            fill="#f5f3ff", stroke="#7c3aed")

arrow(center_bottom(producer), center_top(kafka))
arrow(center_bottom(kafka), center_top(spark))

# ---------------------------------------------------------------------------
# Medallion architecture (shared)
# ---------------------------------------------------------------------------
bronze = box(90, 550, 1520, 85, "BRONZE   (RETAIL_DWH.BRONZE)",
             ["RAW_ORDERS, RAW_CUSTOMERS — source_file + ingested_at retained for lineage"],
             fill="#fef3c7", stroke="#b45309", title_size=16)

arrow(center_bottom(snowpipe), (center_bottom(snowpipe)[0], 550), label="COPY INTO")
arrow(center_bottom(spark), (center_bottom(spark)[0], 550))

silver = box(90, 685, 1200, 90, "SILVER   (RETAIL_DWH.SILVER)",
             ["CLEAN_ORDERS, CUSTOMERS — deduped (latest-wins),", "masking policy applied on EMAIL / PHONE"],
             fill="#f1f5f9", stroke="#475569", title_size=16)

rejected = box(1320, 685, 290, 90, "REJECTED_ORDERS",
               ["QA-failed rows,", "visible, not discarded"],
               fill="#fff7ed", stroke="#fb923c", dashed=True, title_size=13, text_size=11)

cdc = box(1320, 800, 290, 105, "Streams & Tasks (CDC)",
          ["ORDER_CHANGES stream →", "PROCESS_ORDER_CHANGES task →", "SILVER.ORDER_AUDIT"],
          fill="#fff7ed", stroke="#fb923c", dashed=True, title_size=13, text_size=11)

arrow(center_bottom(bronze, 0.4), center_top(silver, 0.4))

# Reject/CDC branches: route through the right margin (x1630/1660, past the
# box's right edge at 1610) so the line never crosses a box's interior/title,
# then turn in at the box's right-edge midpoint.
bx0, by0, bw0, bh0 = bronze
rej_x0, rej_y0, rej_w0, rej_h0 = rejected
cdc_x0, cdc_y0, cdc_w0, cdc_h0 = cdc

multi_arrow(
    [(bx0 + bw0, by0 + bh0 * 0.5), (1630, by0 + bh0 * 0.5),
     (1630, rej_y0 + rej_h0 / 2), (rej_x0 + rej_w0, rej_y0 + rej_h0 / 2)],
    dashed=True, label="rejects", label_seg=1,
)
multi_arrow(
    [(bx0 + bw0, by0 + bh0 * 0.8), (1665, by0 + bh0 * 0.8),
     (1665, cdc_y0 + cdc_h0 / 2), (cdc_x0 + cdc_w0, cdc_y0 + cdc_h0 / 2)],
    dashed=True, label="CDC alt path", label_seg=1,
)

gold = box(90, 850, 1200, 85, "GOLD   (RETAIL_DWH.GOLD)",
           ["DAILY_SALES, CUSTOMER_SEGMENTS — Dynamic Tables, TARGET_LAG refresh"],
           fill="#fef9c3", stroke="#ca8a04", title_size=16)

arrow(center_bottom(silver, 0.4), center_top(gold, 0.4))

bi = box(90, 985, 1200, 65, "BI / Analytics consumption",
         ["Power BI · SQL analytics · Data science"],
         fill="#ecfeff", stroke="#0891b2", title_size=15)

arrow(center_bottom(gold, 0.4), center_top(bi, 0.4))

# ---------------------------------------------------------------------------
# Pipeline C band (governance / ops) across full width
# ---------------------------------------------------------------------------
band_y = 1090
parts.append(f'<rect x="60" y="{band_y}" width="1580" height="120" rx="12" fill="#f0fdfa" stroke="#0f766e" stroke-width="1.5"/>')
section_label(84, band_y + 28, "Pipeline C — Governance & operational monitoring", color="#0f766e", size=16)

gov_items = [
    ("RBAC & Masking", ["06_privacy_policies.sql"]),
    ("PIPELINE_RUN_LOG", ["audit trail"]),
    ("Freshness & anomaly\nchecks", ["freshness_checks.sql"]),
    ("Alerting", ["→ Slack webhook"]),
    ("Infrastructure as code", ["Ansible playbook"]),
    ("CI / CD", ["GitHub Actions"]),
]
gx = 84
gw = (1580 - 2 * 24 - 5 * 16) / 6
for title, lines in gov_items:
    title_lines = title.split("\n")
    b = box(gx, band_y + 42, gw, 62, title_lines[0],
            (title_lines[1:] if len(title_lines) > 1 else []) + lines,
            fill="#ffffff", stroke="#14b8a6", title_size=12.5, text_size=10.5, rx=8)
    gx += gw + 16

# Governance connector: runs down the empty 30px gap between the main
# medallion column (ends x1290) and the CDC/rejected side column (starts
# x1320), so it never crosses a box's interior or title.
gov_line_x = 1305
arrow((gov_line_x, 635), (gov_line_x, band_y), dashed=True, color="#0f766e", marker="arrowhead_teal")
parts.append(
    f'<text x="{gov_line_x + 14}" y="870" font-family="{FONT}" font-size="11.5" '
    f'fill="#0f766e" font-style="italic" text-anchor="middle" '
    f'transform="rotate(-90 {gov_line_x + 14} 870)">observes &amp; governs every layer above</text>'
)

svg = f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">{defs}{"".join(parts)}</svg>'

with open("docs/architecture.svg", "w") as f:
    f.write(svg)

print("wrote docs/architecture.svg", len(svg), "bytes")
