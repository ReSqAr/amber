import os
import glob
import gzip
import json
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px  # Used for accessing color sequences

# 1. Locate the log file with the lexicographically largest name.
files = glob.glob(".amb/logs/run_*.txt.gz")
if not files:
    raise FileNotFoundError("No matching log files found.")
latest_file = sorted(files)[-1]

# 2. Read the file, decode JSON, filter on type after decoding, and convert timestamp.
data = []
try:
    with gzip.open(latest_file, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Skipping line due to JSON decode error: {e}")
                continue
            # Filter on type after decoding.
            if entry.get("type") not in ("sink", "stream"):
                continue
            try:
                entry["dt"] = datetime.fromisoformat(
                    entry["timestamp"].replace("Z", "+00:00")
                )
            except Exception as e:
                print(f"Skipping line due to timestamp error: {e}")
                continue
            data.append(entry)
except EOFError:
    print(
        "Warning: Incomplete file read (EOFError encountered). Proceeding with partial data."
    )

# 3. Group entries by label (id (type)) and sort each group by timestamp.
groups = {}
for entry in data:
    label = f"{entry['id']} ({entry['type']})"
    groups.setdefault(label, []).append(entry)
for label, entries in groups.items():
    entries.sort(key=lambda x: x["dt"])

# 4. Create a Plotly figure with one trace per group.
fig = go.Figure()
colors = px.colors.qualitative.Alphabet
for i, label in enumerate(sorted(groups.keys())):
    entries = groups[label]
    x = [entry["dt"] for entry in entries]
    y = [entry["position"] for entry in entries]
    color = colors[i % len(colors)]
    fig.add_trace(
        go.Scatter(
            x=x,
            y=y,
            mode="lines",
            name=label,
            line=dict(color=color),
            hovertemplate=None,
        )
    )

# 5. Update layout: x-axis shows only the time.
fig.update_layout(
    title="Interactive Log Chart",
    xaxis_title="Time",
    yaxis_title="Position",
    xaxis=dict(tickformat="%H:%M:%S"),
    legend=dict(itemclick="toggleothers"),
    hovermode="x",
    hoverlabel=dict(namelength=-1),
)


fig.show()
