"""Visual system for the guided Spark presentation screen."""

from __future__ import annotations

import streamlit as st


def inject_styles() -> None:
    st.markdown(
        """
        <style>
          :root {
            --paper: #f5f1e8;
            --paper-deep: #e8e1d2;
            --ink: #172321;
            --muted: #64716d;
            --line: #cbd2c9;
            --spark: #e86f28;
            --spark-dark: #9d3f16;
            --spark-soft: #fae3d2;
            --mint: #d9efe7;
          }
          .stApp { background: var(--paper); color: var(--ink); }
          .block-container { max-width: 1180px; padding-top: 2.4rem; padding-bottom: 3rem; }
          h1, h2, h3, h4 { font-family: "Iowan Old Style", "Baskerville", "Times New Roman", serif; color: var(--ink); letter-spacing: -0.025em; }
          p, label, [data-testid="stCaptionContainer"], button { font-family: "Avenir Next", "Segoe UI", sans-serif; }
          .guided-hero { display: flex; align-items: flex-end; justify-content: space-between; gap: 2rem; padding: 1.2rem 1.5rem 1.35rem; border: 1px solid var(--line); border-top: 5px solid var(--spark); background: #fbfaf5; box-shadow: 10px 10px 0 var(--paper-deep); margin-bottom: 1.5rem; }
          .guided-kicker { color: var(--spark-dark); text-transform: uppercase; letter-spacing: .16em; font-size: .72rem; font-weight: 700; margin-bottom: .4rem; }
          .guided-hero h1 { margin: 0; font-size: clamp(1.8rem, 4vw, 3rem); }
          .guided-hero p { margin: .35rem 0 0; color: var(--muted); }
          .guided-status { border-left: 1px solid var(--line); padding-left: 1rem; min-width: 170px; color: var(--muted); font-size: .84rem; }
          .guided-status strong { display: block; color: var(--ink); font-size: 1rem; }
          .stepper { display: grid; grid-template-columns: repeat(7, minmax(0, 1fr)); gap: .35rem; margin: 0 0 1.65rem; }
          .step-node { min-height: 3.5rem; padding: .55rem .35rem .45rem; border-top: 3px solid var(--line); color: var(--muted); font-size: .75rem; line-height: 1.2; }
          .step-node .number { font-family: "Avenir Next", sans-serif; font-weight: 800; margin-right: .25rem; }
          .step-node.done { border-color: var(--spark); color: var(--spark-dark); }
          .step-node.active { border-color: var(--ink); color: var(--ink); background: var(--spark-soft); }
          .step-node.active .number { color: var(--spark-dark); }
          .flow-wrap { border: 1px solid var(--line); background: #fbfaf5; padding: 1rem; margin: .85rem 0 1rem; }
          .flow-label { color: var(--muted); font-size: .72rem; text-transform: uppercase; letter-spacing: .14em; margin-bottom: .65rem; }
          .flow { display: grid; grid-template-columns: 1fr auto 1fr auto 1fr; gap: .55rem; align-items: stretch; }
          .flow-card { display: flex; flex-direction: column; justify-content: center; min-height: 5.2rem; padding: .8rem .9rem; border: 1px solid var(--line); background: #fffdf8; }
          .flow-card.spark { background: var(--spark-soft); border-color: #e7aa84; }
          .flow-card.result { background: var(--mint); border-color: #a8d4c4; }
          .flow-card small { color: var(--muted); text-transform: uppercase; letter-spacing: .12em; font-size: .64rem; }
          .flow-card strong { margin-top: .3rem; font-size: .94rem; line-height: 1.28; }
          .flow-arrow { align-self: center; color: var(--spark); font-size: 1.35rem; font-weight: 700; }
          .evidence-title { color: var(--spark-dark); font-family: "Iowan Old Style", "Baskerville", serif; font-size: 1.2rem; margin: .4rem 0 .35rem; }
          div[data-testid="stMetric"] { background: #fbfaf5; border: 1px solid var(--line); border-left: 4px solid var(--spark); border-radius: 0; padding: .75rem .85rem; }
          div[data-testid="stButton"] > button { border-radius: 0; border: 1px solid var(--ink); min-height: 2.6rem; }
          div[data-testid="stButton"] > button[kind="primary"] { background: var(--ink); color: var(--paper); }
          @media (max-width: 900px) {
            .guided-hero { display: block; box-shadow: 6px 6px 0 var(--paper-deep); }
            .guided-status { border-left: 0; border-top: 1px solid var(--line); padding: .8rem 0 0; margin-top: 1rem; }
            .stepper { grid-template-columns: repeat(4, minmax(0, 1fr)); }
            .flow { grid-template-columns: 1fr; }
            .flow-arrow { transform: rotate(90deg); justify-self: center; }
          }
          @media (max-width: 520px) {
            .block-container { padding-top: 1rem; }
            .stepper { grid-template-columns: repeat(2, minmax(0, 1fr)); }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )
