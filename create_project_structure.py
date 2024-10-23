import os

def create_project_structure():
    # Define folder names
    folders = ['data', 'models', 'notebooks', 'src', 'src/configuration', 'src/connector', 'tests']
    
    # Create folders
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
        print(f"Created folder: {folder}")
    
    # Create files
    files = [
        '.gitignore', 'requirements.txt', 'README.md', 
        'src/__init__.py', 'src/configuration/__init__.py', 'src/connector/__init__.py',
        'data/test.csv', 'models/model.json', 'notebooks/test.ipynb', 'tests/test_cases.txt'
        ]
    for file in files:
        with open(file, 'w') as f:
            f.write('')
        print(f"Created file: {file}")

if __name__ == "__main__":
    create_project_structure()
