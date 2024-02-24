import altair as alt
import duckdb
import pandas as pd
import streamlit as st


# Load raw data from DuckDB
def load_data():
    conn = duckdb.connect("streamlit.duckdb")
    query = """
        SELECT 
            date, 
            cluster, 
            subreddit
        FROM 
            streamlit.reddit
    """
    df = conn.execute(query).fetchdf()
    conn.close()
    df["date"] = pd.to_datetime(df["date"], unit="s")
    return df


# Load cluster information
def load_cluster_info():
    conn = duckdb.connect("streamlit.duckdb")
    query = """
        SELECT 
            cluster,
            title, 
            summary 
        FROM 
            streamlit.cluster_summaries
    """
    cluster_info_df = conn.execute(query).fetchdf()
    conn.close()
    return cluster_info_df


# Load texts for a selected cluster where is_central_member is True
def load_texts_for_cluster(cluster):
    conn = duckdb.connect("streamlit.duckdb")
    query = f"""
        SELECT 
            text
        FROM 
            streamlit.reddit
        WHERE 
            cluster = {cluster} AND is_central_member = True
    """
    texts_df = conn.execute(query).fetchdf()
    conn.close()
    return texts_df


# Create an interactive Altair chart
def create_chart(df):
    highlight = alt.selection(
        type="single", on="mouseover", fields=["cluster"], nearest=True
    )
    base = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X("month:T", axis=alt.Axis(title="date", format="%Y-%m")),
            y="frequency:Q",
            color="cluster:N",
            tooltip=[
                "cluster:N",
                "frequency:Q",
                alt.Tooltip("month:T", title="month", format="%Y-%m"),
            ],
        )
        .properties(width=800, height=400)
    )

    lines = base.mark_line().encode(
        size=alt.condition(~highlight, alt.value(1), alt.value(3))
    )
    points = base.mark_point().encode(
        opacity=alt.condition(highlight, alt.value(1), alt.value(0))
    )

    return (lines + points).add_selection(highlight)


# Main Script Logic
# ==================

# Load data and cluster information
df = load_data()
cluster_info_df = load_cluster_info()

# Process data
clusters_to_include = (
    3,
    5,
    8,
    9,
    10,
    12,
    13,
    15,
    16,
    17,
    18,
    19,
    22,
    23,
    25,
    26,
    28,
    32,
)
df = df[df["cluster"].isin(clusters_to_include)]
df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
monthly_frequencies = (
    df.groupby(["month", "cluster", "subreddit"]).size().reset_index(name="frequency")
)

# Sidebar for subreddit selection
subreddit_list = ["All Subreddits"] + [
    "r/" + subreddit for subreddit in sorted(df["subreddit"].unique().tolist())
]
selected_subreddit = st.sidebar.selectbox("Select a subreddit:", subreddit_list)

# Filter data by selected subreddit
if selected_subreddit != "All Subreddits":
    filtered_df = monthly_frequencies[
        monthly_frequencies["subreddit"] == selected_subreddit[2:]
    ]
else:
    filtered_df = monthly_frequencies

# Display the frequency chart
st.title("Cluster Frequency Over Time")
st.altair_chart(create_chart(filtered_df), use_container_width=True)

# Dropdown for selecting a cluster to view title and summary
cluster_options = ["Select a Cluster"] + sorted(df["cluster"].unique(), key=int)
cluster_options = [str(cluster) for cluster in cluster_options]
selected_cluster_option = st.selectbox("Select a cluster:", cluster_options)

# Display the title and summary for the selected cluster
if selected_cluster_option != "Select a Cluster":
    selected_cluster = int(selected_cluster_option)
    cluster_details = cluster_info_df[cluster_info_df["cluster"] == selected_cluster]
    if not cluster_details.empty:
        cluster_title = cluster_details["title"].iloc[0]
        cluster_summary = cluster_details["summary"].iloc[0]

        st.write(f"**Title:** \n\n{cluster_title}")
        st.write(f"**Summary:** \n\n{cluster_summary}")

        # Load and display texts for the selected cluster
        texts_df = load_texts_for_cluster(selected_cluster)
        if not texts_df.empty:
            st.table(texts_df)
        else:
            st.write("No sample reddit posts are available for this cluster.")
