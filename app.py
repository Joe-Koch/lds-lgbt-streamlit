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


# Function to merge cluster titles into the main DataFrame
def merge_cluster_titles(df, cluster_info_df):
    return pd.merge(df, cluster_info_df[["cluster", "title"]], on="cluster", how="left")


# Load texts for a selected cluster where is_central_member is True
def load_texts_for_cluster(selected_title, cluster_info_df):
    cluster_num = cluster_info_df[cluster_info_df["title"] == selected_title][
        "cluster"
    ].iloc[0]
    conn = duckdb.connect("streamlit.duckdb")
    query = f"""
    SELECT 
        text
    FROM 
        streamlit.reddit
    WHERE 
        cluster = {cluster_num} AND is_central_member = True
    """
    texts_df = conn.execute(query).fetchdf()
    conn.close()
    return texts_df


# Create an interactive Altair chart
def create_chart(df, subreddit):
    chart_title = (
        f"Cluster Frequency Over Time for Subreddit r/{subreddit}"
        if subreddit != "All Subreddits"
        else "Cluster Frequency Over Time"
    )
    highlight = alt.selection(
        type="single", on="mouseover", fields=["title"], nearest=True
    )
    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("frequency:Q", title="Frequency"),
            color=alt.Color("title:N", legend=alt.Legend(title="Cluster Title")),
            tooltip=[
                alt.Tooltip("title:N", title="Cluster Title"),
                alt.Tooltip("frequency:Q", title="Frequency"),
                alt.Tooltip("month:T", title="Month"),
            ],
        )
        .properties(width=800, height=400, title=chart_title)
        .add_selection(highlight)
    )
    return chart


# Main Script
df = load_data()
cluster_info_df = load_cluster_info()
df = merge_cluster_titles(df, cluster_info_df)

clusters_to_include = {
    3,
    5,
    8,
    9,
    10,
    12,
    13,
    15,
    17,
    18,
    19,
    22,
    23,
    25,
    26,
    28,
    32,
}
df = df[df["cluster"].isin(clusters_to_include)]
df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()

monthly_frequencies = (
    df.groupby(["month", "title", "subreddit"]).size().reset_index(name="frequency")
)

# Sidebar for subreddit selection
subreddit_list = ["All Subreddits"] + sorted(df["subreddit"].unique().tolist())
selected_subreddit = st.sidebar.selectbox("Select a subreddit:", subreddit_list)

if selected_subreddit != "All Subreddits":
    filtered_df = monthly_frequencies[
        monthly_frequencies["subreddit"] == selected_subreddit
    ]
else:
    filtered_df = monthly_frequencies

# Display the frequency chart with dynamic title based on selected subreddit
st.altair_chart(create_chart(filtered_df, selected_subreddit), use_container_width=True)

# Dropdown for selecting a cluster to view title and summary
cluster_titles = ["Select a Cluster"] + sorted(
    cluster_info_df[cluster_info_df["cluster"].isin(clusters_to_include)]["title"]
    .unique()
    .tolist()
)
selected_cluster_title = st.selectbox("Select a cluster:", cluster_titles)

# Display the title and summary for the selected cluster
if selected_cluster_title != "Select a Cluster":
    cluster_summary = cluster_info_df[
        cluster_info_df["title"] == selected_cluster_title
    ]["summary"].iloc[0]
    st.write(f"**Summary:** \n{cluster_summary}")

    # Load and display texts for the selected cluster
    texts_df = load_texts_for_cluster(selected_cluster_title, cluster_info_df)
    if not texts_df.empty:
        st.write("**Sample reddit posts and comments for this cluster:**")
        st.table(texts_df)
    else:
        st.write("No sample reddit posts are available for this cluster.")
