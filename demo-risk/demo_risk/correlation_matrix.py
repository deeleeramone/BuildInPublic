from numpy import ones_like, triu
from plotly.graph_objs import Figure, Heatmap, Layout
from pandas import DataFrame, Series


def z_score_standardization(data: Series) -> Series:
    """Z-Score Standardization Method."""
    return (data - data.mean()) / data.std()


def correlation_matrix(
    data, chart: bool = True, theme: str = "dark"
) -> DataFrame | Figure:
    """Correlation Matrix Chart."""
    df = DataFrame(data)

    if "Date" in df.columns:
        df = df.set_index("Date")

    for col in df.columns:
        if col == "Date":
            continue
        df[col] = z_score_standardization(df[col])

    corr = df.corr()

    X = corr.columns.to_list()
    x_replace = X[-1]
    Y = X.copy()
    y_replace = Y[0]
    X = [x if x != x_replace else "" for x in X]
    Y = [y if y != y_replace else "" for y in Y]
    mask = triu(ones_like(corr, dtype=bool))
    df = corr.mask(mask)

    if chart is False:
        return df

    title = ""
    text_color = "white" if theme == "dark" else "black"
    colorscale = "RdBu"

    heatmap = Heatmap(
        z=df,
        x=X,
        y=Y,
        xgap=1,
        ygap=1,
        colorscale=colorscale,
        colorbar=dict(
            orientation="v",
            x=0.9,
            y=0.5,
            xanchor="left",
            yanchor="middle",
            xref="container",
            yref="paper",
            len=0.66,
            bgcolor="rgba(0,0,0,1)" if text_color == "white" else "rgba(255,255,255,1)",
        ),
        text=df.fillna(""),
        texttemplate="%{text:.4f}",
        hoverongaps=False,
        hovertemplate="%{x} - %{y} : %{z:.4f}<extra></extra>",
    )
    layout = Layout(
        title_text=title,
        title_font_size=20,
        title_x=0.5,
        title_y=0.97,
        title_yanchor="top",
        xaxis=dict(
            showgrid=False,
            showline=False,
            ticklen=0,
            tickangle=90,
            automargin=False,
        ),
        yaxis=dict(
            showgrid=False,
            side="left",
            autorange="reversed",
            showline=False,
            ticklen=0,
            automargin="height+width+left",
            tickmode="auto",
        ),
        dragmode="pan",
        paper_bgcolor=(
            "rgba(0,0,0,1)" if text_color == "white" else "rgba(255,255,255,1)"
        ),
        plot_bgcolor=(
            "rgba(0,0,0,1)" if text_color == "white" else "rgba(255,255,255,1)"
        ),
        font=dict(color=text_color),
        margin=dict(t=50, b=75, l=75, r=50),
    )
    fig = Figure(data=[heatmap], layout=layout)

    return fig


def plot_matrix(data, type: str = "correlation", theme: str = "dark"):
    X = data.columns.to_list()
    x_replace = X[-1]
    Y = X.copy()
    y_replace = Y[0]
    X = [x if x != x_replace else "" for x in X]
    Y = [y if y != y_replace else "" for y in Y]
    mask = triu(ones_like(data, dtype=bool))
    df = data.mask(mask)
    text_color = "white" if theme == "dark" else "black"
    colorscale = "RdBu"

    heatmap = Heatmap(
        z=df,
        x=X,
        y=Y,
        xgap=1,
        ygap=1,
        colorscale=colorscale,
        colorbar=dict(
            orientation="v",
            x=0.9,
            y=0.5,
            xanchor="left",
            yanchor="middle",
            xref="container",
            yref="paper",
            len=0.66,
            bgcolor="rgba(0,0,0,1)" if text_color == "white" else "rgba(255,255,255,1)",
        ),
        text=df.fillna(""),
        texttemplate="%{text:.4f}",
        hoverongaps=False,
        hovertemplate="%{x} - %{y} : %{z:.4f}<extra></extra>",
    )
    layout = Layout(
        title_text="",
        xaxis=dict(
            showgrid=False,
            showline=False,
            ticklen=0,
            tickangle=90,
            automargin=False,
        ),
        yaxis=dict(
            showgrid=False,
            side="left",
            autorange="reversed",
            showline=False,
            ticklen=0,
            automargin="height+width+left",
            tickmode="auto",
        ),
        dragmode="pan",
        paper_bgcolor=(
            "rgba(0,0,0,1)" if text_color == "white" else "rgba(255,255,255,1)"
        ),
        plot_bgcolor=(
            "rgba(0,0,0,1)" if text_color == "white" else "rgba(255,255,255,1)"
        ),
        font=dict(color=text_color),
        margin=dict(t=50, b=75, l=75, r=50),
    )
    fig = Figure(data=[heatmap], layout=layout)

    return fig
