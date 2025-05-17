import json
from typing import Optional, Literal

import numpy as np
import pandas as pd
from fastapi import APIRouter
from openbb_core.app.model.abstract.error import OpenBBError
from openbb_charting.core.openbb_figure import OpenBBFigure
from openbb_databento.app.depends import ESOptions
from openbb_core.app.model.obbject import OBBject
from openbb_core.app.utils import df_to_basemodel
from openbb_charting.core.chart_style import ChartStyle
from scipy.spatial import Delaunay


router = APIRouter(prefix="/options")


@router.get("/surface")
def create_surface(
    store: ESOptions,
    date: Literal["2025-05-01", "2024-05-01"] = "2025-05-01",
    option_type: Literal["otm", "itm", "puts", "calls"] = "otm",
    dte_min: Optional[int] = None,
    dte_max: Optional[int] = None,
    moneyness: Optional[float] = None,
    strike_min: Optional[float] = None,
    strike_max: Optional[float] = None,
    theme: Literal["dark", "light"] = "dark",
) -> dict:
    """Chart the volatility as a 3-D surface."""

    cols_map = {
        "expiration": "Expiration",
        "strike_price": "Strike",
        "option_type": "Type",
        "dte": "DTE",
        "implied_volatility": "IV",
    }

    target = "implied_volatility"
    options = store.get(date, "2025-05-01")
    calls = options.query(f"`option_type` == 'call' & `dte` >= 0 & `{target}` > 0")
    puts = options.query(f"`option_type` == 'put' & `dte` >= 0 & `{target}` > 0")

    if dte_min is not None:
        calls = calls.query("`dte` >= @dte_min")
        puts = puts.query("`dte` >= @dte_min")

    if dte_max is not None:
        calls = calls.query("`dte` <= @dte_max")
        puts = puts.query("`dte` <= @dte_max")

    if moneyness is not None and moneyness > 0:
        calls = calls.assign(
            high=lambda df: (1 + (moneyness / 100)) * df["underlying_price"],
            low=lambda df: (1 - (moneyness / 100)) * df["underlying_price"],
        )
        puts = puts.assign(
            high=lambda df: (1 + (moneyness / 100)) * df["underlying_price"],
            low=lambda df: (1 - (moneyness / 100)) * df["underlying_price"],
        )
        calls = calls.query("`low` <= `strike_price` <= `high`").drop(
            columns=["high", "low"]
        )
        puts = puts.query("`low` <= `strike_price` <= `high`").drop(
            columns=["high", "low"]
        )

    if strike_min is not None:
        calls = calls.query("`strike_price` >= @strike_min")
        puts = puts.query("`strike_price` >= @strike_min")

    if strike_max is not None:
        calls = calls.query("`strike_price` <= @strike_max")
        puts = puts.query("`strike_price` <= @strike_max")

    if option_type == "otm":
        otm_calls = calls.query("`strike_price` > `underlying_price`").set_index(
            ["expiration", "strike_price", "option_type"]
        )
        otm_puts = puts.query("`strike_price` < `underlying_price`").set_index(
            ["expiration", "strike_price", "option_type"]
        )
        df = pd.concat([otm_calls, otm_puts]).sort_index().reset_index()

    if option_type == "itm":
        itm_calls = calls.query("`strike_price` < `underlying_price`").set_index(
            ["expiration", "strike_price", "option_type"]
        )
        itm_puts = puts.query("`strike_price` > `underlying_price`").set_index(
            ["expiration", "strike_price", "option_type"]
        )
        df = pd.concat([itm_calls, itm_puts]).sort_index().reset_index()

    if option_type == "calls":
        df = calls
    if option_type == "puts":
        df = puts

    df = df[
        [
            "expiration",
            "strike_price",
            "option_type",
            "dte",
            "implied_volatility",
        ]
    ]

    df = df.rename(columns=cols_map)

    label_dict = {"calls": "Call", "puts": "Put", "otm": "OTM", "itm": "ITM"}
    label = f"Estimated E-Mini S&P 500 {label_dict[option_type]} IV"

    X = df.DTE
    Y = df.Strike
    Z = df.IV

    try:
        points3D = np.vstack((X, Y, Z)).T
        points2D = points3D[:, :2]
        tri = Delaunay(points2D)
        II, J, K = tri.simplices.T
    except Exception as e:
        raise OpenBBError(f"Not enough points to render 3D: {e}")

    fig = OpenBBFigure(create_backend=True)
    fig.update_layout(ChartStyle().plotly_template.get("layout", {}))
    text_color = "white" if "dark" in fig.layout.template else "black"
    fig.set_title(f"{label}")
    fig_kwargs = dict(z=Z, x=X, y=Y, i=II, j=J, k=K, intensity=Z)
    fig.add_mesh3d(
        **fig_kwargs,
        alphahull=0,
        opacity=1,
        contour=dict(color="black", show=True, width=15),
        colorscale=[
            [0, "darkred"],
            [0.001, "crimson"],
            [0.005, "red"],
            [0.0075, "orangered"],
            [0.015, "darkorange"],
            [0.025, "orange"],
            [0.04, "goldenrod"],
            [0.055, "gold"],
            [0.11, "magenta"],
            [0.15, "plum"],
            [0.4, "lightblue"],
            [0.7, "royalblue"],
            [0.9, "blue"],
            [1, "darkblue"],
        ],
        hovertemplate="<b>DTE</b>: %{x} <br><b>Strike</b>: %{y} <br><b>"
        + "IV"
        + "</b>: %{z}<extra></extra>",
        showscale=True,
        flatshading=True,
        lighting=dict(
            ambient=0.95,
            diffuse=0.9,
            roughness=0.8,
            specular=0.9,
            fresnel=0.001,
            vertexnormalsepsilon=0.0001,
            facenormalsepsilon=0.0001,
        ),
    )
    tick_kwargs = dict(tickfont=dict(size=12), titlefont=dict(size=14))
    fig.update_layout(
        scene=dict(
            xaxis=dict(
                backgroundcolor="rgb(94, 94, 94)",
                gridcolor="white",
                showbackground=True,
                zerolinecolor="white",
                title="DTE",
                autorange="reversed",
                **tick_kwargs,
            ),
            yaxis=dict(
                backgroundcolor="rgb(94, 94, 94)",
                gridcolor="white",
                showbackground=True,
                zerolinecolor="white",
                title="Strike",
                **tick_kwargs,
            ),
            zaxis=dict(
                backgroundcolor="rgb(94, 94, 94)",
                gridcolor="white",
                showbackground=True,
                zerolinecolor="white",
                title=cols_map[target],
                **tick_kwargs,
            ),
        ),
        title_x=0.5,
        scene_camera=dict(
            up=dict(x=0, y=0, z=0.75),
            center=dict(x=-0.01, y=0, z=-0.3),
            eye=dict(x=1.75, y=1.75, z=0.69),
        ),
        paper_bgcolor=(
            "rgba(0,0,0,0)" if text_color == "white" else "rgba(255,255,255,255)"
        ),
        plot_bgcolor=(
            "rgba(0,0,0,0)" if text_color == "white" else "rgba(255,255,255,255)"
        ),
        font=dict(color=text_color),
    )

    fig.update_scenes(
        aspectmode="manual",
        aspectratio=dict(x=1.5, y=2.0, z=0.75),
        dragmode="turntable",
    )
    df.Expiration = df.Expiration.astype(str)
    results = df_to_basemodel(df)
    output = OBBject(results=results)
    theme = theme if theme else "dark"
    output.charting._charting_settings.chart_style = theme
    fig = output.charting._set_chart_style(fig)

    content = fig.show(external=True).to_plotly_json()

    content.update(dict(config={"scrollZoom": True, "displayModeBar": True}))

    return json.loads(json.dumps(content))
