import csv
import os
from core.logger import get_logger

logger = get_logger(__name__)

EXPECTED_COLUMNS = 30
INPUT_FILE = "market_evals.csv"
OUTPUT_FILE = "market_evals_clean.csv"

def clean_market_evals_csv(input_path: str = INPUT_FILE, output_path: str = OUTPUT_FILE) -> None:
    """Remove rows that don't have exactly EXPECTED_COLUMNS fields."""
    if not os.path.exists(input_path):
        logger.error(f"\u274c Input file not found: {input_path}")
        return

    with open(input_path, newline='', encoding='utf-8', errors='replace') as infile, \
            open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        try:
            header = next(reader)
        except StopIteration:
            logger.error("\u274c Input CSV is empty.")
            return

        if len(header) != EXPECTED_COLUMNS:
            logger.warning(
                f"\u26A0\ufe0f Header has {len(header)} columns, expected {EXPECTED_COLUMNS}."
            )
        writer.writerow(header[:EXPECTED_COLUMNS])

        kept = 0
        skipped_lines = []
        for line_num, row in enumerate(reader, start=2):
            if len(row) == EXPECTED_COLUMNS:
                writer.writerow(row)
                kept += 1
            else:
                skipped_lines.append(line_num)
                logger.warning(
                    f"Skipping line {line_num}: expected {EXPECTED_COLUMNS} columns, found {len(row)}"
                )

    logger.info(f"\u2705 Wrote {kept} valid rows to {output_path}")
    if skipped_lines:
        logger.info(f"\u23ED\ufe0f Skipped {len(skipped_lines)} malformed rows: {skipped_lines}")

if __name__ == "__main__":
    clean_market_evals_csv()
