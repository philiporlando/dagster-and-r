import base64
import requests
import json
import os
import shutil
from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd

from dagster import (
    AssetExecutionContext,
    MetadataValue,
    asset,
    MaterializeResult,
    PipesSubprocessClient,
    file_relative_path,
    Field, 
    String,
    )

@asset
def hello_world_r(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
) -> MaterializeResult:
    cmd = [shutil.which("Rscript"), file_relative_path(__file__, "./R/hello_world.R")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
    ).get_materialize_result()


@asset(config_schema={"output_dir": Field(String, default_value="./data")})
def iris_r(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
) -> MaterializeResult:
    output_dir = context.op_config["output_dir"]
    cmd = [shutil.which("Rscript"), file_relative_path(__file__, "./R/iris.R")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
        env={
            "MY_ENV_VAR_IN_SUBPROCESS": "This is an environment variable passed from Dagster to R!",
            "OUTPUT_DIR": output_dir,
        },
    ).get_materialize_result()


@asset(deps=[iris_r])
def iris_py(context):
    # TODO replace hardcoded output_dir with resource key
    iris = pd.read_csv(f"data/iris.csv")
    context.log.info(type(iris))
    context.log.info(iris.head())
    return iris


# Assets from the docs included during testing
@asset
def topstory_ids() -> None: # turn it into a function
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_new_story_ids = requests.get(newstories_url).json()[:100]

    os.makedirs("data", exist_ok=True)
    with open("data/topstory_ids.json", "w") as f:
        json.dump(top_new_story_ids, f)


@asset(deps=[topstory_ids])
def topstories(context: AssetExecutionContext) -> MaterializeResult:
    with open("data/topstory_ids.json", "r") as f:
        topstory_ids = json.load(f)

    results = []
    for item_id in topstory_ids:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        ).json()
        results.append(item)

        if len(results) % 20 == 0:
            context.log.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)
    df.to_csv("data/topstories.csv")

    context.log.debug("This is a user-defined debug log!!!")
    context.log.info("This is a user-defined info log!!!")
    context.log.warn("This is a user-defined warning log!!!")
    context.log.error("This is a user-defined error log!!!")
    context.log.critical("This is a user-defined critical log!!!")
    context.log.fatal("This is a user-defined fatal log!!!")
    
    return MaterializeResult(
        metadata={
            "num_records": len(df),  # Metadata can be any key-value pair
            "preview": MetadataValue.md(df.head().to_markdown()),
            # The `MetadataValue` class has useful static methods to build Metadata
        }
    )
    

@asset(deps=[topstories])
def most_frequent_words() -> MaterializeResult:
    stopwords = ["a", "the", "an", "of", "to", "in", "for", "and", "with", "on", "is"]

    topstories = pd.read_csv("data/topstories.csv")

    # loop through the titles and count the frequency of each word
    word_counts = {}
    for raw_title in topstories["title"]:
        title = raw_title.lower()
        for word in title.split():
            cleaned_word = word.strip(".,-!?:;()[]'\"-")
            if cleaned_word not in stopwords and len(cleaned_word) > 0:
                word_counts[cleaned_word] = word_counts.get(cleaned_word, 0) + 1

    # Get the top 25 most frequent words
    top_words = {
        pair[0]: pair[1]
        for pair in sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:25]
    }

    # Make a bar chart of the top 25 words
    plt.figure(figsize=(10, 6))
    plt.bar(list(top_words.keys()), list(top_words.values()))
    plt.xticks(rotation=45, ha="right")
    plt.title("Top 25 Words in Hacker News Titles")
    plt.tight_layout()

    # Convert the image to a saveable format
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    # Convert the image to Markdown to preview it within Dagster
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    with open("data/most_frequent_words.json", "w") as f:
        json.dump(top_words, f)

    # Attach the Markdown content as metadata to the asset
    return MaterializeResult(metadata={"plot": MetadataValue.md(md_content)})
