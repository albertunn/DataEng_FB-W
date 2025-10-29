import os
import shutil
from kaggle.api.kaggle_api_extended import KaggleApi

def download_kaggle_dataset():
    api = KaggleApi()
    api.authenticate()
    print("Authentication successful!")

    dataset = "excel4soccer/espn-soccer-data"
    
    # Download to /tmp first (always writable)
    temp_dir = "/tmp/espn_soccer"
    final_dir = "/opt/airflow/data/espn_soccer"
    
    # Clean up temp directory if it exists
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        print(f"🧹 Cleaned old temp directory")
    
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(final_dir, exist_ok=True)
    
    print(f"\n⬇Downloading entire dataset to {temp_dir}...")
    api.dataset_download_files(dataset, path=temp_dir, unzip=True)
    print(f"Download complete!")
    
    # Files we want to keep
    wanted_files = [
        "fixtures.csv",
        "keyEventDescription.csv",
        "leagues.csv",
        "teamStats.csv",
        "teams.csv",
        "venues.csv",
    ]
    
    print(f"\nLooking for wanted files in base_data folder...")
    base_data_path = os.path.join(temp_dir, "base_data")
    
    if not os.path.exists(base_data_path):
        print(f"base_data folder not found at {base_data_path}")
        print(f"Available folders in temp: {os.listdir(temp_dir)}")
        # Try to find files in root temp dir if base_data doesn't exist
        base_data_path = temp_dir
    
    moved_files = []
    
    # Move only wanted files
    for filename in wanted_files:
        src = os.path.join(base_data_path, filename)
        
        if os.path.exists(src):
            dst = os.path.join(final_dir, filename)
            
            # Remove destination if it exists
            if os.path.exists(dst):
                os.remove(dst)
            
            # Move file
            shutil.copyfile(src, dst)
            os.remove(src)
            size = os.path.getsize(dst)
            print(f"  ✓ Moved {filename} ({size:,} bytes)")
            moved_files.append(filename)
        else:
            print(f"  ✗ {filename} not found at {src}")
    
    # Clean up entire temp directory
    print(f"\n🧹 Cleaning up temp directory...")
    shutil.rmtree(temp_dir)
    print(f"✅ Temp directory deleted")
    
    
    print("\n" + "="*80)
    print(f"✅ SUCCESS: Moved {len(moved_files)} files")
    print("="*80 + "\n")
    
    return moved_files