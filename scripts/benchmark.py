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

# 2. Define configurations dynamically
# By omitting 'cores_per_exec', Spark Standalone will greedily pack cores into 
# a single fat executor per worker node, perfectly handling the odd numbers.
configurations = [
    # VERTICAL BASELINE: 1 core total, constrained memory (800m)
    {"name": "1_core_800m", "total_cores": "1", "memory": "800m"},
    
    # HORIZONTAL RUNS: 2 through 6 cores
    {"name": "2_cores", "total_cores": "2", "memory": "2800m"},
    {"name": "3_cores", "total_cores": "3", "memory": "2800m"},
    {"name": "4_cores", "total_cores": "4", "memory": "2800m"},
    {"name": "5_cores", "total_cores": "5", "memory": "2800m"},
    {"name": "6_cores", "total_cores": "6", "memory": "2800m"}
]

results = {}

print("Starting Spark benchmark tests...")
print(f"Master URL: {master_url}")
print(f"Target Script: {script_path}\n")

# 3. Run the benchmark loop
for conf in configurations:
    print("-" * 50)
    print(f"[Benchmarking] Running config: {conf['name']}")
    print(f"Targeting Total Cores: {conf['total_cores']} | RAM/Executor: {conf['memory']}")
    
    # Construct the spark-submit command without --executor-cores
    cmd = [
        "spark-submit",
        "--master", master_url,
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
print("Benchmarking complete. Generating separate plots...")

# --- 4. Plotting & Analysis Logic ---

v_names = ["1_core_800m", "2_cores"]
h_names = ["2_cores", "3_cores", "4_cores", "5_cores", "6_cores"]

v_runs = [c for c in configurations if c["name"] in v_names and results.get(c["name"]) is not None]
h_runs = [c for c in configurations if c["name"] in h_names and results.get(c["name"]) is not None]

# ---------------------------------------------------------
# A. Vertical Scaling Plots
# ---------------------------------------------------------
if len(v_runs) == 2:
    v_cores = [int(c["total_cores"]) for c in v_runs]
    v_times = [results[c["name"]] for c in v_runs]
    
    t_v_base = v_times[0] 
    v_actual_speedup = [t_v_base / t for t in v_times]
    v_ideal_speedup = [c / v_cores[0] for c in v_cores]

    # Plot 1: Vertical Runtime
    plt.figure(figsize=(8, 5))
    plt.plot(v_cores, v_times, marker='o', color='b', linewidth=2)
    plt.title('Vertical Scaling: Runtime vs. Resources')
    plt.xlabel('Resources Allocated')
    plt.ylabel('Runtime (seconds)')
    plt.xticks(v_cores, ['1 Core (800MB)', '2 Cores (2.8GB total)'])
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "plot_1_vertical_runtime.png"))
    plt.close()

    # Plot 2: Vertical Speedup
    plt.figure(figsize=(8, 5))
    plt.plot(v_cores, v_actual_speedup, marker='o', color='g', linewidth=2, label='Actual Speedup')
    plt.plot(v_cores, v_ideal_speedup, marker='x', linestyle='--', color='r', linewidth=2, label='Ideal Linear Speedup')
    plt.title('Vertical Scaling: Speedup Factor vs. Ideal')
    plt.xlabel('Resources Allocated')
    plt.ylabel('Speedup Factor')
    plt.xticks(v_cores, ['1 Core (800MB)', '2 Cores (2.8GB total)'])
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "plot_2_vertical_speedup.png"))
    plt.close()
    
    print("Saved Vertical Scaling plots (1 and 2).")

# ---------------------------------------------------------
# B. Horizontal Scaling Plots
# ---------------------------------------------------------
if len(h_runs) > 1:
    h_cores = [int(c["total_cores"]) for c in h_runs]
    h_times = [results[c["name"]] for c in h_runs]
    
    t_h_base = h_times[0] 
    h_actual_speedup = [t_h_base / t for t in h_times]
    h_ideal_speedup = [c / h_cores[0] for c in h_cores]

    # Plot 3: Horizontal Runtime
    plt.figure(figsize=(8, 5))
    plt.plot(h_cores, h_times, marker='o', color='b', linewidth=2)
    plt.title('Horizontal Scaling: Runtime vs. Total Cores')
    plt.xlabel('Total Cores Across Cluster')
    plt.ylabel('Runtime (seconds)')
    plt.xticks(h_cores)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "plot_3_horizontal_runtime.png"))
    plt.close()

    # Plot 4: Horizontal Speedup
    plt.figure(figsize=(8, 5))
    plt.plot(h_cores, h_actual_speedup, marker='o', color='g', linewidth=2, label='Actual Speedup')
    plt.plot(h_cores, h_ideal_speedup, marker='x', linestyle='--', color='r', linewidth=2, label='Ideal Linear Speedup')
    plt.title('Horizontal Scaling: Speedup Factor vs. Ideal')
    plt.xlabel('Total Cores Across Cluster')
    plt.ylabel('Speedup Factor ($Time_{base} / Time_N$)')
    plt.xticks(h_cores)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "plot_4_horizontal_speedup.png"))
    plt.close()
    
    print("Saved Horizontal Scaling plots (3 and 4).")

print("\nAll tasks completed successfully!")