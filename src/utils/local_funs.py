# # Create directory if it doesn't exist in local testing
# def create_dir_if_not_exists(dir_path: Path):
#     if not dir_path.exists():
#         dir_path.mkdir(parents=True, exist_ok=True)
#         print(f"Directory {dir_path} created.")
#     else:
#         print(f"Directory {dir_path} already exists.")

# # Set the paths
# def set_paths():
#     root = pathlib.Path(__file__).parent.parent.absolute()
#     TABLE_PATH = Path(root, 'test', "data", "raw")
#     create_dir_if_not_exists(TABLE_PATH)
#     return TABLE_PATH