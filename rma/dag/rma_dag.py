# pylint: disable=pointless-statement


from rma.dag.dag import DAG
from rma.dag.sink import Sink
from rma.dag.source import Source
from rma.dag.worker import Worker
from rma.dag.joiner import DAGJoiner
from rma.dag.ventilator import VentilatorBlock


def build_rma_dag(nworkers: int = 3):
    # ===================================================================== Start
    dag = DAG("DAG")
    posts_source = Source("posts_source", "zmqrelay", ["9999"])
    comments_source = Source("comments_source", "zmqrelay", ["9999"])

    # ===================================================================== Posts top path
    filter_posts_cols_top = VentilatorBlock(
        "filter_posts_cols_top",
        "transform filter-columns",
        ["id", "url"],
        nworkers=nworkers,
    )
    filter_null_url = VentilatorBlock(
        "filter_null_url", "filter null-url", nworkers=nworkers
    )

    # ===================================================================== Posts middle path
    filter_posts_cols_middle = VentilatorBlock(
        "filter_posts_cols_middle", "transform filter-columns", ["id", "score"]
    )
    posts_score_mean = Worker("posts_score_mean", "transform posts-score-mean")

    # ===================================================================== Posts bottom
    filter_posts_cols_bottom = VentilatorBlock(
        "filter_posts_cols_bottom",
        "transform filter-columns",
        ["score", "id", "url"],
        nworkers=nworkers,
    )
    filter_posts_above_mean_score = Worker(
        "filter_posts_above_mean_score", "filter posts-score-above-mean"
    )
    filter_null_url_bottom = VentilatorBlock(
        "filter_null_url_bottom", "filter null-url", nworkers=nworkers
    )

    # ===================================================================== Comments bottom
    filter_comments_cols_bottom = VentilatorBlock(
        "filter_comments_cols_bottom",
        "transform filter-columns",
        ["permalink", "body"],
        nworkers=nworkers,
    )
    filter_ed_comments = VentilatorBlock(
        "filter_ed_comments", "filter ed-comments", nworkers=nworkers
    )
    extract_post_id_bottom = VentilatorBlock(
        "extract_post_id_bottom", "transform extract-post-id", nworkers=nworkers
    )
    filter_unique_posts = Worker("filter_unique_posts", "filter uniq-posts")

    # ===================================================================== Comments side
    filter_comments_cols_side = VentilatorBlock(
        "filter_comments_cols_side",
        "transform filter-columns",
        ["permalink", "sentiment"],
        nworkers=nworkers,
    )
    filter_nan_sentiment = VentilatorBlock(
        "filter_nan_sentiment", "filter nan-sentiment", nworkers=nworkers
    )
    extract_post_id_side = VentilatorBlock(
        "extract_post_id_side", "transform extract-post-id", nworkers=nworkers
    )
    mean_sentiment = Worker("mean_sentiment", "transform mean-sentiment")

    # ===================================================================== JOIN
    join_dump_posts_urls = DAGJoiner("join_dump_posts_urls", "bykey", ["id"])
    join_download_meme = DAGJoiner("join_download_meme", "bykey", ["id"])

    # ===================================================================== Sink
    memes_url_sink = Sink("sink_memes_url", "zmqsink", ["9999"],)
    mean_posts_score_sink = Sink("sink_mean_posts_score", "zmqsink", ["9999"],)
    download_meme_sink = Sink("sink_download_meme", "zmq-top-post", ["9999"],)

    dag >> posts_source
    dag >> comments_source

    posts_source >> filter_posts_cols_top >> filter_null_url >> join_download_meme
    posts_source >> filter_posts_cols_middle >> posts_score_mean
    (
        posts_source
        >> filter_posts_cols_bottom
        >> filter_posts_above_mean_score
        >> filter_null_url_bottom
        >> join_dump_posts_urls
    )
    posts_score_mean > filter_posts_above_mean_score  # add dependency

    (
        comments_source
        >> filter_comments_cols_bottom
        >> filter_ed_comments
        >> extract_post_id_bottom
        >> filter_unique_posts
        >> join_dump_posts_urls
    )
    (
        comments_source
        >> filter_comments_cols_side
        >> filter_nan_sentiment
        >> extract_post_id_side
        >> mean_sentiment
        >> join_download_meme
    )

    join_download_meme >> download_meme_sink
    join_dump_posts_urls >> memes_url_sink
    posts_score_mean >> mean_posts_score_sink

    return dag
