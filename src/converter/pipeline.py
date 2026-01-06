"""ETL Pipeline orchestration."""

from pathlib import Path

from .extractors.base import Extractor
from .transformers.base import Transformer
from .loaders.base import Loader


class ETLPipeline:
    """Orchestrates the ETL process."""

    def __init__(
        self,
        extractor: Extractor,
        transformer: Transformer,
        loader: Loader,
    ):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self, input_path: Path, output_path: Path) -> int:
        """Run the pipeline.
        
        Returns count of processed records.
        """
        def transform_all():
            for source in self.extractor.extract(input_path):
                result = self.transformer.transform(source)
                if result is not None:
                    yield result

        return self.loader.load(transform_all(), output_path)
