import os
import glob
import gzip
import json
from datetime import datetime
import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objects as go
import plotly.express as px

# --- Utility functions ---


def get_log_files():
    files = sorted(glob.glob(".amb/logs/run_*.txt.gz"))
    return files


def create_figure_from_file(log_file):
    data = []
    try:
        with gzip.open(log_file, "rt") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if entry.get("type") not in ("sink", "stream"):
                    continue
                try:
                    entry["dt"] = datetime.fromisoformat(
                        entry["timestamp"].replace("Z", "+00:00")
                    )
                except Exception:
                    continue
                data.append(entry)
    except EOFError:
        print("Warning: EOFError encountered; using partial data.")

    groups = {}
    for entry in data:
        label = f"{entry['id']} ({entry['type']})"
        groups.setdefault(label, []).append(entry)

    for label, entries in groups.items():
        entries.sort(key=lambda x: x["dt"])

    fig = go.Figure()
    colors = px.colors.qualitative.Alphabet
    for i, label in enumerate(sorted(groups.keys())):
        entries = groups[label]
        fig.add_trace(
            go.Scatter(
                x=[entry["dt"] for entry in entries],
                y=[entry["position"] for entry in entries],
                mode="lines",
                name=label,
                line=dict(color=colors[i % len(colors)]),
                hovertemplate=None,
            )
        )

    fig.update_layout(
        title=f"Interactive Log Chart - {os.path.basename(log_file)}",
        xaxis_title="Time",
        yaxis_title="Position",
        xaxis=dict(tickformat="%H:%M:%S"),
        legend=dict(itemclick="toggleothers"),
        hovermode="x",
        hoverlabel=dict(namelength=-1),
    )
    return fig


# --- Dash setup ---

app = dash.Dash(__name__)
server = app.server

# --- Initial files & default file ---
initial_files = get_log_files()
default_file = initial_files[-1] if initial_files else None

# --- Layout ---
app.layout = html.Div(
    [
        html.Div(
            [
                dcc.Dropdown(
                    id="file-dropdown",
                    options=[
                        {"label": os.path.basename(f), "value": f}
                        for f in initial_files
                    ],
                    value=default_file,
                    clearable=False,
                    style={"width": "1200px"},
                ),
                html.Button("Refresh Files", id="refresh-button", n_clicks=0),
                html.Button("Redraw Graph", id="redraw-button", n_clicks=0),
            ],
            style={
                "display": "flex",
                "alignItems": "center",
                "gap": "10px",
                "marginBottom": "20px",
            },
        ),
        dcc.Loading(
            id="loading-graph",
            type="circle",
            fullscreen=False,
            children=dcc.Graph(id="log-graph", style={"height": "90vh"}),
        ),
    ]
)

# --- Callbacks ---


@app.callback(
    [Output("file-dropdown", "options"), Output("file-dropdown", "value")],
    Input("refresh-button", "n_clicks"),
)
def update_file_list(n_clicks):
    files = get_log_files()
    options = [{"label": os.path.basename(f), "value": f} for f in files]
    value = files[-1] if files else None
    return options, value


@app.callback(
    Output("log-graph", "figure"),
    Input("redraw-button", "n_clicks"),
    Input("file-dropdown", "value"),
)
def update_graph(n_clicks_redraw, selected_file):
    if selected_file:
        print(f"Rendering graph for: {selected_file}")
        return create_figure_from_file(selected_file)
    else:
        return go.Figure()


if __name__ == "__main__":
    app.run(debug=True)
