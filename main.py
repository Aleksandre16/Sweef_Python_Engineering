from etl_pipeline import etl_pipeline
import asyncio

if __name__ == "__main__":
    asyncio.run(etl_pipeline())
    print("მონაცემები წარმატებით ჩაიტვირთა ბაზაში!")