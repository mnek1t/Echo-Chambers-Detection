import streamlit as st
import subprocess
import os
from pathlib import Path

REPORT_DIR = os.getenv("REPORT_DIR", "")

st.title("Echo Chamber Analysis")

run_analysis = st.button("Run analysis")

if run_analysis:
    st.subheader("Execution")

    log_container = st.expander("Debug / Logs", expanded=True)
    log_box = log_container.empty()

    logs = ""

    with st.spinner("Running analysis... this may take a few minutes"):
        process = subprocess.Popen(
            ["python", "metrics/community_detection.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        for line in process.stdout:
            logs += line
            log_box.code(logs)

        process.wait()

    if process.returncode != 0:
        st.error("Analysis failed")
        st.stop()

    st.success("Analysis completed successfully")


    st.subheader("Results")

    images = [
        "ecs_by_community.png",
        "conductance_by_community.png",
        "variance_by_community.png",
    ]

    for img in images:
        img_path = Path(REPORT_DIR) / img
        if img_path.exists():
            st.image(str(img_path), caption=img)
        else:
            st.warning(f"Missing file: {img}")
