import streamlit as st
import subprocess
import os
from pathlib import Path
from neo4j import GraphDatabase
import time

REPORT_DIR = os.getenv("REPORT_DIR", "")
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

num_users = 0
num_posts = 0

st.title("Echo Chamber Analysis")
stats_user_container = st.empty()
stats_posts_container = st.empty()


def update_stats():
    with driver.session() as session:
        num_users = session.run(
            "MATCH (u:User) RETURN count(u) AS c"
        ).single()["c"]
        num_posts = session.run(
            "MATCH (p:Post) RETURN count(p) AS c"
        ).single()["c"]

    stats_user_container.metric("Number of users", num_users)
    stats_posts_container.metric("Number of posts", num_posts)


update_stats()

if st.button("Update stats"):
    update_stats()

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
