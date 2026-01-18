"""Project utilities."""
from processing_datalake.hooks import get_context
from kedro.io.core import (
    AbstractDataset,
)


def get_dataset(
    dataset_name: str,
) -> AbstractDataset:
    """Get a Kedro dataset by name only if a Kedro Session is previously loaded.

    Args:
        dataset_name (str): The name of the dataset to retrieve.

    Returns:
        AbstractDataset: The requested Kedro dataset.
    """

    try:
        context = get_context()
        catalog = context.catalog
        return catalog._get_dataset(dataset_name)
    except RuntimeError:
        raise RuntimeError("There is no DataCatalog available.")
