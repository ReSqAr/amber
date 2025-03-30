import os
import glob
import gzip
import io
from datetime import datetime
import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

# --- Utility functions ---


def get_log_files():
    # Newest files first.
    return sorted(glob.glob(".amb/logs/run_*.txt.gz"), reverse=True)


# Subclass gzip.GzipFile to override read and ignore EOFError
class SafeGzipFile(gzip.GzipFile):
    def read(self, *args, **kwargs):
        try:
            return super().read(*args, **kwargs)
        except EOFError:
            return b""


def create_safe_text_reader(filepath):
    """Opens a gzip file using SafeGzipFile and wraps it as a text stream."""
    safe_gzip = SafeGzipFile(filepath, mode="rb")
    return io.TextIOWrapper(safe_gzip, encoding="utf-8")


class CompleteLineReader:
    """
    A file-like iterator that yields only complete lines (i.e. those ending in a newline).
    If an EOFError occurs (i.e. the file is truncated), iteration stops gracefully.
    """

    def __init__(self, text_file):
        self.text_file = text_file

    def __iter__(self):
        while True:
            try:
                line = self.text_file.readline()
            except EOFError:
                break  # Stop iteration on EOFError
            if not line:
                break
            if line.endswith("\n"):
                yield line

    def read(self):
        return "".join(list(self.__iter__()))


def create_figure_from_file(log_file, mode="progress", chunksize=1000000):
    """
    mode: 'progress' or 'delay'
      - In progress mode: plots 'position' over time for both sink and stream.
      - In delay mode: plots 'delay_ns' over time for sinks only.
    """
    dfs = []  # list to accumulate DataFrame chunks
    with create_safe_text_reader(log_file) as f:
        filtered_reader = CompleteLineReader(f)
        try:
            # Use Pandas to read newline-delimited JSON in chunks.
            chunk_iter = pd.read_json(filtered_reader, lines=True, chunksize=chunksize)
        except ValueError as e:
            print(f"Error initializing JSON reader for {log_file}: {e}")
            return {}
        for chunk in chunk_iter:
            # Convert timestamp to datetime using inferred format.
            chunk["dt"] = pd.to_datetime(
                chunk["timestamp"], utc=True, errors="coerce"
            )
            # Drop rows with unparseable dates.
            chunk = chunk[chunk["dt"].notna()]
            if mode == "progress":
                # Keep sink and stream.
                chunk = chunk[chunk["type"].isin(["sink", "stream"])].copy()
                if chunk.empty:
                    continue
                # Create a label combining id and type.
                chunk["label"] = chunk["id"].astype(str) + " (" + chunk["type"] + ")"
            else:  # delay mode
                # Keep only sink entries with delay_ns.
                chunk = chunk[(chunk["type"] == "sink") & (chunk["delay_ns"].notna())].copy()
                if chunk.empty:
                    continue
                # Label by sink id.
                chunk["delay_s"] = chunk["delay_ns"] / 1e9
                chunk["label"] = "Sink " + chunk["id"].astype(str)
            dfs.append(chunk)
    if not dfs:
        return {}  # Return an empty dict (no figure) if no data was collected.
    df = pd.concat(dfs, ignore_index=True)
    df.sort_values("dt", inplace=True)

    if mode == "progress":
        fig = px.line(
            df,
            x="dt",
            y="position",
            color="label",
            title=f"Interactive Log Chart - {os.path.basename(log_file)} (Progress Mode)",
        )
        yaxis_title = "Position"
    else:  # delay mode
        fig = px.line(
            df,
            x="dt",
            y="delay_s",
            color="label",
            title=f"Interactive Log Chart - {os.path.basename(log_file)} (Delay Mode)",
        )
        yaxis_title = "Delay (s)"

    # Update traces to disable the default hover template.
    fig.update_traces(hovertemplate=None)
    # Update layout similar to your original configuration.
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title=yaxis_title,
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
default_file = initial_files[0] if initial_files else None

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
                    style={"width": "400px"},
                ),
                html.Button("Refresh Files", id="refresh-button", n_clicks=0),
                html.Button("Redraw Graph", id="redraw-button", n_clicks=0),
                dcc.RadioItems(
                    id="mode-selector",
                    options=[
                        {"label": "Progress", "value": "progress"},
                        {"label": "Delay", "value": "delay"},
                    ],
                    value="progress",
                    labelStyle={"display": "inline-block", "margin-right": "20px"},
                ),
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


@app.callback(
    [Output("file-dropdown", "options"), Output("file-dropdown", "value")],
    Input("refresh-button", "n_clicks"),
)
def update_file_list(n_clicks):
    files = get_log_files()
    options = [{"label": os.path.basename(f), "value": f} for f in files]
    value = files[0] if files else None
    return options, value


@app.callback(
    Output("log-graph", "figure"),
    Input("redraw-button", "n_clicks"),
    Input("file-dropdown", "value"),
    Input("mode-selector", "value"),
)
def update_graph(n_clicks_redraw, selected_file, mode):
    if selected_file:
        print(f"Rendering graph for: {selected_file} in mode: {mode}")
        return create_figure_from_file(selected_file, mode=mode)
    return {}


if __name__ == "__main__":
    app.run(debug=True)
