# app_main.py
"""
Tkinter GUI for the MariaDB Flight Anomaly Detector project.

Features:
- Buttons for Load Data, Basic Anomalies, WF Anomalies, and Generate & Validate Reports.
- Large text output box showing logs and summary from each script.
- Runs tasks asynchronously to keep the UI responsive.
- No charts/images displayed (only textual output).
- Safe for GitHub: no credentials or sensitive data included.
"""

import tkinter as tk
from tkinter import scrolledtext, messagebox
import threading
from config import PATHS

# Import your backend scripts
from scripts_wf import (
    _03_load_data,
    _04_detect_anomalies_wf,
    _05_detect_anomalies_wf,
    _runvalidation_report,  # unified generate+validate script
)


# ------------------ Threaded Execution ------------------

def run_in_thread(func, output_box, button_list):
    """Run the given function in a separate thread so GUI stays responsive."""
    def task():
        try:
            for b in button_list:
                b.config(state="disabled")

            output_box.delete(1.0, tk.END)
            output_box.insert(tk.END, f"🚀 Running {func.__name__}...\n\n")

            result = func()
            output_box.insert(tk.END, result + "\n")
            output_box.see(tk.END)

            messagebox.showinfo("Completed", f"{func.__name__} finished successfully!")
        except Exception as e:
            output_box.insert(tk.END, f"❌ Error: {e}\n")
            output_box.see(tk.END)
            messagebox.showerror("Error", str(e))
        finally:
            for b in button_list:
                b.config(state="normal")

    threading.Thread(target=task).start()


# ------------------ GUI Setup ------------------

def main():
    root = tk.Tk()
    root.title("✈️ MariaDB Flight Anomaly Detector")
    root.geometry("900x700")
    root.config(bg="#f5f5f5")

    # Title
    title_label = tk.Label(
        root,
        text="MariaDB Flight Anomaly Detector",
        font=("Segoe UI", 18, "bold"),
        bg="#f5f5f5",
        fg="#333",
    )
    title_label.pack(pady=10)

    # Output box
    output_box = scrolledtext.ScrolledText(
        root,
        width=110,
        height=25,
        wrap=tk.WORD,
        font=("Consolas", 10),
        bg="white",
        fg="black",
    )
    output_box.pack(padx=10, pady=10)

    # Buttons
    button_frame = tk.Frame(root, bg="#f5f5f5")
    button_frame.pack(pady=10)

    buttons = [
        ("Load Data", _03_load_data.run),
        ("Detect Basic Anomalies", _04_detect_anomalies_wf.run),
        ("Detect WF Anomalies", _05_detect_anomalies_wf.run),
        ("Generate & Validate Reports", _runvalidation_report.run),
    ]

    button_widgets = []
    for label, func in buttons:
        btn = tk.Button(
            button_frame,
            text=label,
            width=25,
            height=2,
            bg="#4CAF50",
            fg="white",
            font=("Segoe UI", 10, "bold"),
            command=lambda f=func: run_in_thread(f, output_box, button_widgets),
        )
        btn.pack(side=tk.LEFT, padx=8, pady=5)
        button_widgets.append(btn)

    # Exit button
    exit_btn = tk.Button(
        root,
        text="Exit",
        width=12,
        height=1,
        bg="#e74c3c",
        fg="white",
        font=("Segoe UI", 10, "bold"),
        command=root.destroy,
    )
    exit_btn.pack(pady=10)

    root.mainloop()


if __name__ == "__main__":
    main()
