"""Industrial-editorial visual system for the Spark data lab."""

from __future__ import annotations

import streamlit as st


def inject_styles() -> None:
    st.markdown(
        """
        <style>
          :root {
            --lab-paper: #f2efe7;
            --lab-panel: #fbfaf6;
            --lab-ink: #14201e;
            --lab-muted: #66716d;
            --lab-line: #b8c0ba;
            --lab-faint: #dfe2dc;
            --lab-spark: #ed5b24;
            --lab-spark-soft: #fbe0d2;
            --lab-ids: #17624b;
            --lab-ids-soft: #d9ebe2;
          }

          .stApp {
            background-color: var(--lab-paper);
            background-image: linear-gradient(rgba(20, 32, 30, .025) 1px, transparent 1px);
            background-size: 100% 28px;
            color: var(--lab-ink);
          }
          .block-container { max-width: 1120px; padding-top: 1.5rem; padding-bottom: 3.5rem; }
          h1, h2, h3, h4 { font-family: "Bodoni MT", "Didot", "Rockwell", serif; color: var(--lab-ink); }
          p, label, button, [data-testid="stCaptionContainer"], [data-testid="stDataFrame"] {
            font-family: "Aptos", "Trebuchet MS", sans-serif;
          }

          .lab-hero {
            border-top: 7px solid var(--lab-ink);
            border-bottom: 1px solid var(--lab-ink);
            padding: .75rem 0 1rem;
            margin-bottom: 1.25rem;
          }
          .lab-brand {
            color: var(--lab-spark);
            font-family: "Bahnschrift", "Trebuchet MS", sans-serif;
            font-size: .76rem;
            font-weight: 800;
            letter-spacing: .17em;
          }
          .lab-brand span { color: var(--lab-muted); margin-left: .55rem; font-weight: 600; }
          .lab-hero-row { display: grid; grid-template-columns: minmax(0, 1fr) 190px; gap: 2rem; align-items: end; }
          .lab-hero h1 { font-size: clamp(2rem, 4.2vw, 3.7rem); line-height: .98; max-width: 760px; margin: .65rem 0 0; letter-spacing: -.045em; }
          .hero-explainer { max-width: 680px; margin: .7rem 0 0; color: var(--lab-muted); font-size: .92rem; }
          .run-stamp { border-left: 3px solid var(--lab-spark); padding: .25rem 0 .2rem .8rem; overflow-wrap: anywhere; }
          .run-stamp small { display: block; color: var(--lab-muted); font-size: .62rem; letter-spacing: .13em; }
          .run-stamp strong { display: block; margin: .15rem 0; font-size: .9rem; }
          .run-stamp span { color: var(--lab-muted); font-size: .7rem; }

          .stage-rail { display: grid; grid-template-columns: repeat(3, 1fr); border: 1px solid var(--lab-ink); margin-bottom: .6rem; }
          .lab-stage { min-height: 4.3rem; padding: .65rem .8rem; border-right: 1px solid var(--lab-ink); background: var(--lab-panel); }
          .lab-stage:last-child { border-right: 0; }
          .lab-stage span { display: block; color: var(--lab-muted); font: 700 .7rem/1 "Bahnschrift", sans-serif; margin-bottom: .45rem; }
          .lab-stage strong { display: block; font: 700 .9rem/1.2 "Aptos", "Trebuchet MS", sans-serif; }
          .lab-stage.done span { color: var(--lab-spark); }
          .lab-stage.active { background: var(--lab-ink); color: var(--lab-panel); box-shadow: inset 0 -5px 0 var(--lab-spark); }
          .lab-stage.active strong, .lab-stage.active span { color: var(--lab-panel); }

          .step-ticks { display: grid; grid-template-columns: repeat(7, minmax(0, 1fr)); gap: 1px; background: var(--lab-line); border: 1px solid var(--lab-line); margin-bottom: 1.7rem; }
          .step-tick { min-height: 2.6rem; padding: .45rem .5rem; background: var(--lab-paper); color: var(--lab-muted); font: 600 .65rem/1.15 "Aptos", sans-serif; }
          .step-tick b { color: var(--lab-muted); margin-right: .3rem; }
          .step-tick.passed b { color: var(--lab-spark); }
          .step-tick.active { background: var(--lab-spark-soft); color: var(--lab-ink); box-shadow: inset 0 -3px 0 var(--lab-spark); }

          .step-heading { display: flex; align-items: baseline; gap: .8rem; margin-bottom: 1.1rem; }
          .step-heading span { color: var(--lab-spark); font: 800 .68rem/1 "Bahnschrift", sans-serif; letter-spacing: .11em; text-transform: uppercase; }
          .step-heading strong { color: var(--lab-ink); font: 700 1rem/1.2 "Aptos", sans-serif; }

          .section-tag { margin: 1.1rem 0 .45rem; color: var(--lab-muted); font: 800 .66rem/1 "Bahnschrift", sans-serif; letter-spacing: .13em; }
          .section-tag::before { content: ""; display: inline-block; width: 18px; height: 3px; margin: 0 .45rem .15rem 0; background: var(--lab-ink); }
          .section-tag.spark-tag { color: var(--lab-spark); }
          .section-tag.spark-tag::before, .section-tag.result-tag::before { background: var(--lab-spark); }
          .section-tag.ids-tag { color: var(--lab-ids); }
          .section-tag.ids-tag::before { background: var(--lab-ids); }
          .lab-question { max-width: 850px; margin: 0 0 .7rem; font: 700 clamp(1.55rem, 3vw, 2.45rem)/1.08 "Bodoni MT", "Didot", serif; letter-spacing: -.025em; }

          .diagram-tag { color: var(--lab-ink); margin-top: 1.35rem; }
          .execution-diagram { display: grid; grid-template-columns: minmax(105px, 1fr) auto minmax(105px, 1fr) auto minmax(155px, 1.45fr) auto minmax(130px, 1.1fr); gap: .45rem; align-items: stretch; margin: .2rem 0 1rem; padding: .7rem; border: 1px dashed var(--lab-line); background: rgba(251, 250, 246, .72); }
          .diagram-node { min-width: 0; padding: .7rem .6rem; border: 1px solid var(--lab-ink); background: var(--lab-panel); }
          .diagram-node strong, .diagram-node span { display: block; }
          .diagram-node strong { font: 700 .88rem/1.2 "Aptos", "Trebuchet MS", sans-serif; overflow-wrap: anywhere; }
          .diagram-node span { margin-top: .35rem; color: var(--lab-muted); font-size: .72rem; line-height: 1.25; }
          .diagram-arrow { align-self: center; color: var(--lab-spark); font-size: 1.2rem; font-weight: 800; }

          div[data-testid="stDataFrame"] { border: 1px solid var(--lab-ink); background: var(--lab-panel); }
          div[data-testid="stCode"] { border: 0; border-left: 5px solid var(--lab-spark); border-radius: 0; }
          div[data-testid="stCode"] pre { min-height: 104px; display: flex; align-items: center; }
          .reading-note { margin: .35rem 0 1.2rem; padding: .65rem .8rem; border-left: 3px solid var(--lab-spark); background: var(--lab-spark-soft); color: var(--lab-ink); font-size: .86rem; }
          .plain-answer { min-height: 5rem; padding: .8rem .9rem; border-top: 1px solid var(--lab-ink); background: var(--lab-panel); font-size: .94rem; line-height: 1.45; }
          .plain-answer.ids-answer { border-top-color: var(--lab-ids); background: var(--lab-ids-soft); }

          details { border-top: 1px solid var(--lab-line) !important; border-bottom: 1px solid var(--lab-line) !important; border-radius: 0 !important; margin-top: 1.1rem; }
          div[data-testid="stButton"] > button,
          div[data-testid="stLinkButton"] > a { border-radius: 0; border: 1px solid var(--lab-ink); min-height: 2.7rem; font-weight: 700; }
          div[data-testid="stButton"] > button[kind="primary"] { background: var(--lab-ink); color: var(--lab-panel); }
          button:focus-visible, a:focus-visible { outline: 3px solid var(--lab-spark) !important; outline-offset: 2px; }

          @media (max-width: 820px) {
            .lab-hero-row { grid-template-columns: 1fr; gap: 1rem; }
            .run-stamp { max-width: 260px; }
            .step-ticks { grid-template-columns: repeat(4, minmax(0, 1fr)); }
            .step-tick:nth-child(4) { border-right: 0; }
            .execution-diagram { grid-template-columns: 1fr; }
            .diagram-arrow { transform: rotate(90deg); }
          }
          @media (max-width: 560px) {
            .block-container { padding-top: .9rem; }
            .stage-rail { grid-template-columns: 1fr; }
            .lab-stage { min-height: auto; border-right: 0; border-bottom: 1px solid var(--lab-ink); }
            .lab-stage:last-child { border-bottom: 0; }
            .step-ticks { grid-template-columns: repeat(2, minmax(0, 1fr)); }
            .step-heading { display: block; }
            .step-heading strong { display: block; margin-top: .35rem; }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )
