import subprocess
import time
import matplotlib.pyplot as plt
import os
import sys

# 1. Setup absolute paths so the script works from anywhere
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_dir = os.path.join(project_root, "src")
sys.path.append(src_dir)

import config

# Ensure the docs directory exists for saving the plots
output_dir = os.path.join(project_root, "docs")
os.makedirs(output_dir, exist_ok=True)

script_path = os.path.join(src_dir, "etl_job.py")
master_url = config.SPARK_MASTER

# 2. Define configurations with exact memory limits in megabytes
configurations = [
    # 1 worker, 1 cpu, 1.4gb of memory (represented as 1400m)
    {
        "name": "1_worker_1core_1400m", 
        "workers": 1, "cores_per_worker": "1", "total_cores": "1", "memory": "1400m", 
        "type": "vertical"
    },
    # 1 worker, 2 cpu, 2.8gb of memory (represented as 2800m)
    {
        "name": "1_worker_2core_2800m", 
        "workers": 1, "cores_per_worker": "2", "total_cores": "2", "memory": "2800m", 
        "type": "horizontal"
    },
    # 2 workers, 2 cpu (each), 2.8gb mem (each)
    {
        "name": "2_workers_2core_2800m", 
        "workers": 2, "cores_per_worker": "2", "total_cores": "4", "memory": "2800m", 
        "type": "horizontal"
    },
    # 3 workers, 2 cpu (each), 2.8gb mem (each)
    {
        "name": "3_workers_2core_2800m", 
        "workers": 3, "cores_per_worker": "2", "total_cores": "6", "memory": "2800m", 
        "type": "horizontal"
    }
]

results = {}

print("Starting Spark benchmark tests...")
print(f"Master URL: {master_url}")
print(f"Target Script: {script_path}\n")

# 3. Run the benchmark loop
for conf in configurations:
    print("-" * 50)
    print(f"[Benchmarking] Running config: {conf['name']}")
    print(f"Targeting: {conf['workers']} worker(s) | Cores/Worker: {conf['cores_per_worker']} | Total Cores: {conf['total_cores']} | RAM/Worker: {conf['memory']}")
    
    # Construct the spark-submit command
    cmd = [
        "spark-submit",
        "--master", master_url,
        "--executor-cores", conf["cores_per_worker"],
        "--total-executor-cores", conf["total_cores"],
        "--executor-memory", conf["memory"],
        script_path
    ]
    
    start_time = time.time()
    
    # Run the job. capture_output=True keeps your console clean
    process = subprocess.run(cmd, capture_output=True, text=True) 
    
    end_time = time.time()
    
    if process.returncode == 0:
        duration = end_time - start_time
        results[conf["name"]] = duration
        print(f"Success! Runtime: {duration:.2f} seconds")
    else:
        print(f"Job failed for {conf['name']}. Here is the end of the error log:")
        print(process.stderr[-1000:]) 
        results[conf["name"]] = None

print("\n" + "="*50)
print("Benchmarking complete. Generating plots and summaries...")

# --- 4. Plotting & Analysis Logic ---

# A. Horizontal Scaling (Node scaling)
h_configs = [c for c in configurations if c["type"] == "horizontal" and results[c["name"]] is not None]

if len(h_configs) == 3:
    workers = [c["workers"] for c in h_configs]
    runtimes = [results[c["name"]] for c in h_configs]
    
    # Calculate Speedup (Time_1 / Time_N)
    t1 = runtimes[0] 
    actual_speedup = [t1 / t for t in runtimes]
    ideal_speedup = workers # Ideal linear speedup matches the number of workers

    plt.figure(figsize=(12, 5))

    # Plot 1: Runtime vs Workers
    plt.subplot(1, 2, 1)
    plt.plot(workers, runtimes, marker='o', linestyle='-', color='b', linewidth=2)
    plt.title('Runtime vs. Number of Workers (2 Cores / 2.8GB each)')
    plt.xlabel('Number of Workers')
    plt.ylabel('Runtime (seconds)')
    plt.xticks(workers)
    plt.grid(True, linestyle='--', alpha=0.7)

    # Plot 2: Speedup vs Ideal Linear Speedup
    plt.subplot(1, 2, 2)
    plt.plot(workers, actual_speedup, marker='o', linestyle='-', color='g', linewidth=2, label='Actual Speedup')
    plt.plot(workers, ideal_speedup, marker='x', linestyle='--', color='r', linewidth=2, label='Ideal Linear Speedup')
    plt.title('Speedup Factor vs. Ideal')
    plt.xlabel('Number of Workers')
    plt.ylabel('Speedup Factor ($Time_1 / Time_N$)')
    plt.xticks(workers)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)

    plt.tight_layout()
    
    # Save the figure
    plot_path = os.path.join(output_dir, "horizontal_scaling_results.png")
    plt.savefig(plot_path)
    print(f"Horizontal scaling plots successfully saved to: {plot_path}")
else:
    print("Skipping horizontal plots: Not all horizontal runs completed successfully.")

# B. Vertical Scaling (Core/RAM scaling)
t_low = results.get("1_worker_1core_1400m")
t_high = results.get("1_worker_2core_2800m")

if t_low and t_high:
    print("\n--- Vertical Scaling Results ---")
    print(f"1 Core / 1.4GB RAM Runtime:  {t_low:.2f} seconds")
    print(f"2 Cores / 2.8GB RAM Runtime: {t_high:.2f} seconds")
    print(f"Speedup Factor: {t_low / t_high:.2f}x (Ideal is ~2.0x)")