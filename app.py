import streamlit as st
import pandas as pd
import altair as alt
import duckdb


# Function to load data from DuckDB
def load_data():
    conn = duckdb.connect("streamlit.duckdb")
    query = """
    SELECT 
        date, 
        cluster, 
        subreddit,
        COUNT(*) AS frequency
    FROM 
        streamlit.reddit
    GROUP BY 
        date, cluster, subreddit
    ORDER BY 
        date ASC
    """
    df = conn.execute(query).fetchdf()
    conn.close()
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


# Interactive Altair chart
def create_chart(df):
    highlight = alt.selection(
        type="single", on="mouseover", fields=["cluster"], nearest=True
    )
    base = (
        alt.Chart(df)
        .encode(
            x="date:T",
            y="frequency:Q",
            color="cluster:N",
            tooltip=["cluster:N", "frequency:Q", "date:T"],
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


df = load_data()
cluster_info_df = load_cluster_info()
df["date"] = pd.to_datetime(df["date"], unit="s")

# Prepare the subreddit selection dropdown
subreddit_list = ["All Subreddits"] + [
    "r/" + subreddit for subreddit in sorted(df["subreddit"].unique().tolist())
]
selected_subreddit = st.sidebar.selectbox("Select a subreddit:", subreddit_list)

if selected_subreddit != "All Subreddits":
    filtered_df = df[
        df["subreddit"] == selected_subreddit[2:]
    ]  # Adjusting for "r/" prefix
else:
    filtered_df = df

# Make the frequency plot
st.title("Cluster Frequency Over Time")
st.altair_chart(create_chart(filtered_df), use_container_width=True)

# Dropdown for selecting a cluster
cluster_options = ["Select a Cluster"] + sorted(df["cluster"].unique(), key=int)
cluster_options = [str(cluster) for cluster in cluster_options]
selected_cluster_option = st.selectbox("Select a cluster:", cluster_options)

# Display the title and summary for the selected cluster
if selected_cluster_option != "Select a Cluster":
    selected_cluster = int(
        selected_cluster_option
    )  # Convert selected cluster back to int for filtering
    if selected_cluster in cluster_info_df["cluster"].values:
        # Fetching the title and summary for the selected cluster
        cluster_title = cluster_info_df[cluster_info_df["cluster"] == selected_cluster][
            "title"
        ].iloc[0]
        cluster_summary = cluster_info_df[
            cluster_info_df["cluster"] == selected_cluster
        ]["summary"].iloc[0]

        # Displaying the title and summary
        st.write(f"**Title:** {cluster_title}")
        st.write(f"**Summary:** {cluster_summary}")

        # Load and display texts where is_central_member is True for the selected cluster
        texts_df = load_texts_for_cluster(selected_cluster)
        if not texts_df.empty:
            st.write("**Sample reddit posts and comments:**")
            st.table(texts_df)
        else:
            st.write(
                "No sample reddit posts and comments are available for this cluster."
            )
    else:
        st.write("No information available for the selected cluster.")
