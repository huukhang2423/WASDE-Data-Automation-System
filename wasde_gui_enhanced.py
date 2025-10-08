import os
import pandas as pd
import glob
import datetime
import time
import subprocess
import logging
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog, font
from pathlib import Path
import sys
from PIL import Image, ImageTk  # Cần cài đặt: pip install pillow

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wasde_automation.log'),
        logging.StreamHandler()
    ]
)

# Cấu hình
CONFIG = {
    "data_folder": r"C:\Users\Admin\Desktop\Project- đạt",
    "output_file": r"C:\Users\Admin\Desktop\Project- đạt\filtered_data.csv",
    "stocks_ratio_file": r"C:\Users\Admin\Desktop\Project- đạt\stocks_ratio.csv",
    "total_supply_est_file": r"C:\Users\Admin\Desktop\Project- đạt\Total_supply_EST.csv",
    "total_supply_proj_file": r"C:\Users\Admin\Desktop\Project- đạt\Total_supply_PROJ.csv",
    "powerbi_file": r"C:\Users\Admin\Desktop\Project- đạt\Dash Đạt.pbix",
    # Danh sách các ReportTitle cần lọc
    "report_titles": [
        "World Coarse Grain Supply and Use",
        "World Corn Supply and Use",
        "World Cotton Supply and Use",
        "World Rice Supply and Use  (Milled Basis)",
        "World Soybean Meal Supply and Use",
        "World Soybean Oil Supply and Use",
        "World Soybean Supply and Use",
        "World Wheat Supply and Use"
    ],
    # Danh sách các Attribute cần lọc
    "attributes": [
        "Beginning Stocks",
        "Domestic Total",
        "Exports",
        "Imports",
        "Production",
        "Ending Stocks",
        "Domestic Use"
    ]
}

# Màu sắc và giao diện - Bảng màu Material Design hiện đại
COLORS = {
    "primary": "#2196F3",          # Xanh dương đậm (Material Blue)
    "primary_light": "#64B5F6",    # Xanh dương nhạt
    "primary_dark": "#1565C0",     # Xanh dương đậm
    "secondary": "#4CAF50",        # Xanh lá (Material Green)
    "accent": "#FF9800",           # Cam (Material Orange)
    "warning": "#FFC107",          # Vàng (Material Amber)
    "danger": "#F44336",           # Đỏ (Material Red)
    "success": "#66BB6A",          # Xanh lá nhạt
    "info": "#29B6F6",             # Xanh dương thông tin (Light Blue)
    "background": "#FAFAFA",       # Xám rất nhạt (Material Grey 50)
    "card": "#FFFFFF",             # Trắng
    "header": "#1976D2",           # Xanh dương header
    "text": "#212121",             # Đen nhạt (Material Grey 900)
    "text_secondary": "#757575",   # Xám đậm (Material Grey 600)
    "text_disabled": "#BDBDBD",    # Xám nhạt (Material Grey 400)
    "border": "#E0E0E0",           # Xám rất nhạt cho viền (Material Grey 300)
    "hover": "#E3F2FD"             # Xanh dương rất nhạt (Blue 50) cho hiệu ứng hover
}

# Tạo các hàm xử lý dữ liệu
def create_stocks_ratio_csv(input_file, output_file):
    """Tạo file stocks_ratio.csv từ dữ liệu trong filtered_data.csv."""
    try:
        # Kiểm tra file input có tồn tại không
        if not os.path.exists(input_file):
            logging.error(f"Không tìm thấy file {input_file}")
            return False
        
        # Đọc file CSV
        logging.info(f"Đang đọc file {input_file} để tạo stocks ratio...")
        df = pd.read_csv(input_file)
        logging.info(f"Đã đọc file thành công với {df.shape[0]} dòng và {df.shape[1]} cột.")
        
        # Kiểm tra các cột cần thiết
        required_columns = ['ReportDate', 'Commodity', 'Region', 'Attribute', 'Value', 'ProjEstFlag']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logging.error(f"Thiếu các cột sau trong dữ liệu: {missing_columns}")
            logging.error(f"Các cột hiện có: {', '.join(df.columns.tolist())}")
            return False
        
        # Lọc dữ liệu với ProjEstFlag là "Proj."
        logging.info("Đang lọc dữ liệu với ProjEstFlag = 'Proj.'...")
        proj_df = df[df['ProjEstFlag'] == 'Proj.'].copy()
        
        if proj_df.empty:
            logging.warning("Không có dữ liệu nào thỏa mãn điều kiện ProjEstFlag = 'Proj.'")
            return False
        
        logging.info(f"Đã lọc được {proj_df.shape[0]} dòng với ProjEstFlag = 'Proj.'")
        
        # Tạo một DataFrame trống để lưu kết quả với 5 cột yêu cầu
        result_df = pd.DataFrame(columns=['ReportDate', 'Region', 'Commodity', 'ProjEstFlag', 'ratio'])
        count_successful = 0
        count_skipped = 0
        
        # Lấy danh sách các nhóm unique của (ReportDate, Commodity, Region)
        groups = proj_df[['ReportDate', 'Commodity', 'Region']].drop_duplicates()
        
        # Xử lý từng nhóm
        logging.info(f"Đang xử lý {len(groups)} nhóm dữ liệu...")
        for index, group in groups.iterrows():
            report_date = group['ReportDate']
            commodity = group['Commodity']
            region = group['Region']
            
            # Lọc dữ liệu cho nhóm hiện tại
            group_data = proj_df[
                (proj_df['ReportDate'] == report_date) & 
                (proj_df['Commodity'] == commodity) & 
                (proj_df['Region'] == region)
            ]
            
            # Tìm giá trị của các thuộc tính
            ending_stocks = group_data[group_data['Attribute'] == 'Ending Stocks']['Value'].values
            exports = group_data[group_data['Attribute'] == 'Exports']['Value'].values
            domestic_total = group_data[group_data['Attribute'] == 'Domestic Total']['Value'].values
            
            # Kiểm tra xem có đủ dữ liệu không
            if (len(ending_stocks) == 0 or len(exports) == 0 or len(domestic_total) == 0):
                logging.warning(f"Bỏ qua {commodity} - {region}: Thiếu một hoặc nhiều thuộc tính cần thiết")
                count_skipped += 1
                continue
            
            # Tính tỷ lệ với công thức mới: Ending Stocks / (Exports + Domestic Total)
            try:
                denominator = exports[0] + domestic_total[0]
                if denominator == 0:
                    logging.warning(f"Bỏ qua {commodity} - {region}: Mẫu số bằng 0")
                    count_skipped += 1
                    continue
                    
                ratio = ending_stocks[0] / denominator
                count_successful += 1
            except Exception as e:
                logging.error(f"Lỗi khi tính tỷ lệ cho {commodity} - {region}: {str(e)}")
                count_skipped += 1
                continue
            
            # Tạo dòng kết quả mới (chỉ với 5 cột yêu cầu)
            new_row = {
                'ReportDate': report_date,
                'Region': region,
                'Commodity': commodity,
                'ProjEstFlag': 'Proj.',  # Giữ giá trị ProjEstFlag như yêu cầu
                'ratio': ratio           # Đổi tên từ Value thành ratio
            }
            
            # Thêm vào DataFrame kết quả
            result_df = pd.concat([result_df, pd.DataFrame([new_row])], ignore_index=True)
        
        logging.info(f"Đã xử lý thành công {count_successful} nhóm, bỏ qua {count_skipped} nhóm")
        
        # Nếu không có dữ liệu nào được xử lý thành công
        if result_df.empty:
            logging.error("Không có dữ liệu nào được xử lý thành công")
            return False
        
        # Lưu kết quả vào file CSV
        logging.info(f"Đang lưu kết quả với {result_df.shape[0]} dòng vào file {output_file}...")
        result_df.to_csv(output_file, index=False)
        logging.info(f"Đã lưu kết quả thành công!")
        return True
    except Exception as e:
        logging.error(f"Lỗi khi tạo file stocks_ratio.csv: {str(e)}")
        return False

def create_total_supply_est_csv(input_file, output_file):
    """Tạo file Total_supply_EST.csv từ dữ liệu trong filtered_data.csv."""
    try:
        # Kiểm tra file input có tồn tại không
        if not os.path.exists(input_file):
            logging.error(f"Không tìm thấy file {input_file}")
            return False
        
        # Đọc file CSV
        logging.info(f"Đang đọc file {input_file} để tạo Total Supply Est...")
        df = pd.read_csv(input_file)
        logging.info(f"Đã đọc file thành công với {df.shape[0]} dòng và {df.shape[1]} cột.")
        
        # Kiểm tra các cột cần thiết
        required_columns = ['ReportDate', 'Commodity', 'Region', 'Attribute', 'Value', 'ProjEstFlag']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logging.error(f"Thiếu các cột sau trong dữ liệu: {missing_columns}")
            logging.error(f"Các cột hiện có: {', '.join(df.columns.tolist())}")
            return False
        
        # Lọc dữ liệu với ProjEstFlag là "Est."
        logging.info("Đang lọc dữ liệu với ProjEstFlag = 'Est.'...")
        est_df = df[df['ProjEstFlag'] == 'Est.'].copy()
        
        if est_df.empty:
            logging.warning("Không có dữ liệu nào thỏa mãn điều kiện ProjEstFlag = 'Est.'")
            return False
        
        logging.info(f"Đã lọc được {est_df.shape[0]} dòng với ProjEstFlag = 'Est.'")
        
        # Tạo một DataFrame trống để lưu kết quả
        result_df = pd.DataFrame(columns=['ReportDate', 'Region', 'Commodity', 'ProjEstFlag', 'total_supply'])
        count_successful = 0
        count_skipped = 0
        
        # Lấy danh sách các nhóm unique của (ReportDate, Commodity, Region)
        groups = est_df[['ReportDate', 'Commodity', 'Region']].drop_duplicates()
        
        # Xử lý từng nhóm
        logging.info(f"Đang xử lý {len(groups)} nhóm dữ liệu...")
        for index, group in groups.iterrows():
            report_date = group['ReportDate']
            commodity = group['Commodity']
            region = group['Region']
            
            # Lọc dữ liệu cho nhóm hiện tại
            group_data = est_df[
                (est_df['ReportDate'] == report_date) & 
                (est_df['Commodity'] == commodity) & 
                (est_df['Region'] == region)
            ]
            
            # Tìm giá trị của các thuộc tính
            beginning_stocks = group_data[group_data['Attribute'] == 'Beginning Stocks']['Value'].values
            production = group_data[group_data['Attribute'] == 'Production']['Value'].values
            
            # Kiểm tra xem có đủ dữ liệu không
            if (len(beginning_stocks) == 0 or len(production) == 0):
                logging.warning(f"Bỏ qua {commodity} - {region}: Thiếu một hoặc nhiều thuộc tính cần thiết")
                count_skipped += 1
                continue
            
            # Tính tổng cung: Beginning Stocks + Production
            try:
                total_supply = beginning_stocks[0] + production[0]
                count_successful += 1
            except Exception as e:
                logging.error(f"Lỗi khi tính tổng cung cho {commodity} - {region}: {str(e)}")
                count_skipped += 1
                continue
            
            # Tạo dòng kết quả mới
            new_row = {
                'ReportDate': report_date,
                'Region': region,
                'Commodity': commodity,
                'ProjEstFlag': 'Est.',
                'total_supply': total_supply
            }
            
            # Thêm vào DataFrame kết quả
            result_df = pd.concat([result_df, pd.DataFrame([new_row])], ignore_index=True)
        
        logging.info(f"Đã xử lý thành công {count_successful} nhóm, bỏ qua {count_skipped} nhóm")
        
        # Nếu không có dữ liệu nào được xử lý thành công
        if result_df.empty:
            logging.error("Không có dữ liệu Est. nào được xử lý thành công")
            return False
        
        # Lưu kết quả vào file CSV
        logging.info(f"Đang lưu kết quả với {result_df.shape[0]} dòng vào file {output_file}...")
        result_df.to_csv(output_file, index=False)
        logging.info(f"Đã lưu kết quả Total Supply Est thành công!")
        return True
    except Exception as e:
        logging.error(f"Lỗi khi tạo file Total_supply_EST.csv: {str(e)}")
        return False

def create_total_supply_proj_csv(input_file, output_file):
    """Tạo file Total_supply_PROJ.csv từ dữ liệu trong filtered_data.csv."""
    try:
        # Kiểm tra file input có tồn tại không
        if not os.path.exists(input_file):
            logging.error(f"Không tìm thấy file {input_file}")
            return False
        
        # Đọc file CSV
        logging.info(f"Đang đọc file {input_file} để tạo Total Supply Proj...")
        df = pd.read_csv(input_file)
        logging.info(f"Đã đọc file thành công với {df.shape[0]} dòng và {df.shape[1]} cột.")
        
        # Kiểm tra các cột cần thiết
        required_columns = ['ReportDate', 'Commodity', 'Region', 'Attribute', 'Value', 'ProjEstFlag']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logging.error(f"Thiếu các cột sau trong dữ liệu: {missing_columns}")
            logging.error(f"Các cột hiện có: {', '.join(df.columns.tolist())}")
            return False
        
        # Lọc dữ liệu với ProjEstFlag là "Proj."
        logging.info("Đang lọc dữ liệu với ProjEstFlag = 'Proj.'...")
        proj_df = df[df['ProjEstFlag'] == 'Proj.'].copy()
        
        if proj_df.empty:
            logging.warning("Không có dữ liệu nào thỏa mãn điều kiện ProjEstFlag = 'Proj.'")
            return False
        
        logging.info(f"Đã lọc được {proj_df.shape[0]} dòng với ProjEstFlag = 'Proj.'")
        
        # Tạo một DataFrame trống để lưu kết quả
        result_df = pd.DataFrame(columns=['ReportDate', 'Region', 'Commodity', 'ProjEstFlag', 'total_supply'])
        count_successful = 0
        count_skipped = 0
        
        # Lấy danh sách các nhóm unique của (ReportDate, Commodity, Region)
        groups = proj_df[['ReportDate', 'Commodity', 'Region']].drop_duplicates()
        
        # Xử lý từng nhóm
        logging.info(f"Đang xử lý {len(groups)} nhóm dữ liệu...")
        for index, group in groups.iterrows():
            report_date = group['ReportDate']
            commodity = group['Commodity']
            region = group['Region']
            
            # Lọc dữ liệu cho nhóm hiện tại
            group_data = proj_df[
                (proj_df['ReportDate'] == report_date) & 
                (proj_df['Commodity'] == commodity) & 
                (proj_df['Region'] == region)
            ]
            
            # Tìm giá trị của các thuộc tính
            beginning_stocks = group_data[group_data['Attribute'] == 'Beginning Stocks']['Value'].values
            production = group_data[group_data['Attribute'] == 'Production']['Value'].values
            
            # Kiểm tra xem có đủ dữ liệu không
            if (len(beginning_stocks) == 0 or len(production) == 0):
                logging.warning(f"Bỏ qua {commodity} - {region}: Thiếu một hoặc nhiều thuộc tính cần thiết")
                count_skipped += 1
                continue
            
            # Tính tổng cung: Beginning Stocks + Production
            try:
                total_supply = beginning_stocks[0] + production[0]
                count_successful += 1
            except Exception as e:
                logging.error(f"Lỗi khi tính tổng cung cho {commodity} - {region}: {str(e)}")
                count_skipped += 1
                continue
            
            # Tạo dòng kết quả mới
            new_row = {
                'ReportDate': report_date,
                'Region': region,
                'Commodity': commodity,
                'ProjEstFlag': 'Proj.',
                'total_supply': total_supply
            }
            
            # Thêm vào DataFrame kết quả
            result_df = pd.concat([result_df, pd.DataFrame([new_row])], ignore_index=True)
        
        logging.info(f"Đã xử lý thành công {count_successful} nhóm, bỏ qua {count_skipped} nhóm")
        
        # Nếu không có dữ liệu nào được xử lý thành công
        if result_df.empty:
            logging.error("Không có dữ liệu Proj. nào được xử lý thành công")
            return False
        
        # Lưu kết quả vào file CSV
        logging.info(f"Đang lưu kết quả với {result_df.shape[0]} dòng vào file {output_file}...")
        result_df.to_csv(output_file, index=False)
        logging.info(f"Đã lưu kết quả Total Supply Proj thành công!")
        return True
    except Exception as e:
        logging.error(f"Lỗi khi tạo file Total_supply_PROJ.csv: {str(e)}")
        return False

class CustomLogger(logging.Handler):
    """Xử lý log với màu sắc theo mức độ"""
    def __init__(self, text_widget):
        logging.Handler.__init__(self)
        self.text_widget = text_widget
        self.level_colors = {
            logging.DEBUG: COLORS["text_secondary"],
            logging.INFO: COLORS["text"],
            logging.WARNING: COLORS["warning"],
            logging.ERROR: COLORS["danger"],
            logging.CRITICAL: COLORS["danger"]
        }
        self.level_tags = {
            logging.DEBUG: "debug",
            logging.INFO: "info",
            logging.WARNING: "warning",
            logging.ERROR: "error",
            logging.CRITICAL: "critical"
        }

    def emit(self, record):
        msg = self.format(record)
        tag = self.level_tags.get(record.levelno, "info")
        
        self.text_widget.config(state=tk.NORMAL)
        self.text_widget.insert(tk.END, msg + "\n", tag)
        self.text_widget.see(tk.END)
        self.text_widget.config(state=tk.DISABLED)
        self.text_widget.update()

class ModernWasdeApp:
    """Ứng dụng WASDE với giao diện hiện đại"""
    def __init__(self, root):
        self.root = root
        self.root.title("WASDE Data Automation Tool")
        self.root.geometry("1000x720")
        self.root.minsize(900, 650)
        
        # Cấu hình màu nền chính
        self.root.configure(bg=COLORS["background"])
        
        # Tạo style
        self.setup_styles()
        
        # Tạo layout chính
        self.create_layout()
        
        # Tạo các widget trong từng khu vực
        self.create_header()
        self.create_config_section()
        self.create_action_section()
        self.create_log_section()
        self.create_footer()
        
        # Thiết lập logger
        self.setup_logger()
        
        # Biến theo dõi tiến trình
        self.processing = False
    
    def setup_styles(self):
        """Thiết lập styles cho giao diện hiện đại"""
        self.style = ttk.Style()
        
        # Sử dụng theme mặc định
        # Lưu ý: Có thể lựa chọn 'clam', 'alt', 'default', 'classic' tùy thuộc vào hệ điều hành
        try:
            self.style.theme_use("clam")  # Theme hiện đại hơn
        except tk.TclError:
            pass  # Nếu không có theme này, sử dụng theme mặc định
        
        # Tùy chỉnh các widget
        
        # Frame styles
        self.style.configure("TFrame", background=COLORS["background"])
        self.style.configure("Card.TFrame", background=COLORS["card"])
        self.style.configure("Header.TFrame", background=COLORS["header"])
        
        # Label styles
        self.style.configure("TLabel", 
                            background=COLORS["background"], 
                            foreground=COLORS["text"],
                            font=("Segoe UI", 10))
        
        self.style.configure("Card.TLabel", 
                            background=COLORS["card"], 
                            foreground=COLORS["text"])
        
        self.style.configure("Header.TLabel", 
                            background=COLORS["header"], 
                            foreground="white",
                            font=("Segoe UI", 16, "bold"))
        
        self.style.configure("Subtitle.TLabel", 
                            background=COLORS["card"], 
                            foreground=COLORS["text_secondary"],
                            font=("Segoe UI", 9))
        
        self.style.configure("Title.TLabel", 
                            background=COLORS["card"], 
                            foreground=COLORS["text"],
                            font=("Segoe UI", 12, "bold"))
        
        # Button styles
        self.style.configure("TButton", 
                            font=("Segoe UI", 10),
                            padding=5)
        
        self.style.map("TButton",
                      background=[('active', COLORS["primary_light"])],
                      foreground=[('active', "white")])
        
        # Primary button (xanh dương)
        self.style.configure("Primary.TButton", 
                            background=COLORS["primary"],
                            foreground="white",
                            padding=10)
        
        self.style.map("Primary.TButton",
                      background=[('active', COLORS["primary_dark"])],
                      foreground=[('active', "white")],
                      relief=[('pressed', 'sunken')])
        
        # Accent button (cam)
        self.style.configure("Accent.TButton", 
                            background=COLORS["accent"],
                            foreground="white",
                            padding=10)
        
        self.style.map("Accent.TButton",
                      background=[('active', "#FB8C00")],  # Cam đậm hơn
                      foreground=[('active', "white")])
        
        # Danger button (đỏ)
        self.style.configure("Danger.TButton", 
                            background=COLORS["danger"],
                            foreground="white",
                            padding=10)
        
        self.style.map("Danger.TButton",
                      background=[('active', "#D32F2F")],  # Đỏ đậm hơn
                      foreground=[('active', "white")])
        
        # Success button (xanh lá)
        self.style.configure("Success.TButton", 
                            background=COLORS["success"],
                            foreground="white",
                            padding=10)
        
        self.style.map("Success.TButton",
                      background=[('active', "#388E3C")],  # Xanh lá đậm hơn
                      foreground=[('active', "white")])
        
        # Outline button (trong suốt, có viền)
        self.style.configure("Outline.TButton", 
                            background=COLORS["card"],
                            foreground=COLORS["primary"],
                            padding=5)
        
        self.style.map("Outline.TButton",
                      background=[('active', COLORS["hover"])],
                      foreground=[('active', COLORS["primary_dark"])])
        
        # Entry styles
        self.style.configure("TEntry", 
                            padding=8,
                            fieldbackground=COLORS["card"])
        
        # Checkbutton styles
        self.style.configure("TCheckbutton",
                            background=COLORS["card"],
                            foreground=COLORS["text"],
                            font=("Segoe UI", 10))
        
        self.style.map("TCheckbutton",
                      background=[('active', COLORS["card"])],
                      foreground=[('active', COLORS["primary"])])
        
        # Progressbar styles
        self.style.configure("TProgressbar",
                            background=COLORS["primary"],
                            troughcolor=COLORS["border"],
                            thickness=15)
    
    def create_layout(self):
        """Tạo layout chính cho ứng dụng"""
        # Main container
        self.main_container = ttk.Frame(self.root, style="TFrame")
        self.main_container.pack(fill=tk.BOTH, expand=True, padx=0, pady=0)
        
        # Header area
        self.header_frame = ttk.Frame(self.main_container, style="Header.TFrame")
        self.header_frame.pack(fill=tk.X, padx=0, pady=0)
        
        # Content area - sử dụng grid để quản lý layout
        self.content_frame = ttk.Frame(self.main_container, style="TFrame", padding=(15, 15, 15, 10))
        self.content_frame.pack(fill=tk.BOTH, expand=True, padx=0, pady=0)
        
        # Chia layout thành 2 phần chính
        self.content_frame.columnconfigure(0, weight=1)
        self.content_frame.rowconfigure(0, weight=0)  # Phần cấu hình
        self.content_frame.rowconfigure(1, weight=0)  # Phần hành động
        self.content_frame.rowconfigure(2, weight=1)  # Phần log
        
        # Khung cấu hình
        self.config_frame = ttk.Frame(self.content_frame, style="Card.TFrame", padding=15)
        self.config_frame.grid(row=0, column=0, sticky="nsew", padx=0, pady=(0, 15))
        
        # Khung hành động
        self.action_frame = ttk.Frame(self.content_frame, style="Card.TFrame", padding=15)
        self.action_frame.grid(row=1, column=0, sticky="nsew", padx=0, pady=(0, 15))
        
        # Khung log
        self.log_frame = ttk.Frame(self.content_frame, style="Card.TFrame", padding=15)
        self.log_frame.grid(row=2, column=0, sticky="nsew", padx=0, pady=0)
        
        # Footer area
        self.footer_frame = ttk.Frame(self.main_container, style="TFrame", padding=(15, 5, 15, 10))
        self.footer_frame.pack(fill=tk.X, side=tk.BOTTOM, padx=0, pady=0)
    
    def create_header(self):
        """Tạo phần header với logo và tiêu đề"""
        # Logo (có thể thay bằng logo thực tế)
        logo_label = ttk.Label(self.header_frame, text="WASDE", 
                              font=("Segoe UI", 20, "bold"), 
                              foreground="white",
                              background=COLORS["header"])
        logo_label.pack(side=tk.LEFT, padx=20, pady=15)
        
        # Tiêu đề
        title_label = ttk.Label(self.header_frame, 
                               text="Data Automation Tool", 
                               style="Header.TLabel")
        title_label.pack(side=tk.LEFT, padx=0, pady=15)
        
        # Phiên bản
        version_label = ttk.Label(self.header_frame, 
                                text="v2.0", 
                                font=("Segoe UI", 10),
                                foreground="white",
                                background=COLORS["header"])
        version_label.pack(side=tk.RIGHT, padx=20, pady=15)
    
    def create_config_section(self):
        """Tạo phần cấu hình với các trường nhập liệu"""
        # Tiêu đề section
        title_label = ttk.Label(self.config_frame, text="Cấu hình", style="Title.TLabel")
        title_label.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 10))
        
        # Tạo separator
        separator = ttk.Separator(self.config_frame, orient="horizontal")
        separator.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(0, 15))
        
        # Thiết lập grid cho config_frame
        self.config_frame.columnconfigure(0, weight=0)  # Label
        self.config_frame.columnconfigure(1, weight=1)  # Entry
        self.config_frame.columnconfigure(2, weight=0)  # Button
        
        # Các trường cấu hình
        row = 2  # Bắt đầu từ hàng thứ 2 (sau title và separator)
        
        # 1. Thư mục dữ liệu
        ttk.Label(self.config_frame, text="Thư mục dữ liệu:", 
                 style="Card.TLabel").grid(row=row, column=0, sticky="w", padx=(0, 10), pady=(0, 10))
        
        self.data_folder_var = tk.StringVar(value=CONFIG["data_folder"])
        data_folder_entry = ttk.Entry(self.config_frame, textvariable=self.data_folder_var, width=60)
        data_folder_entry.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=(0, 10))
        
        browse_button = ttk.Button(self.config_frame, text="Chọn", 
                                  command=self.select_data_folder)
        browse_button.grid(row=row, column=2, padx=(0, 0), pady=(0, 10))
        
        row += 1
        
        # 2. File đầu ra
        ttk.Label(self.config_frame, text="File đầu ra:", 
                 style="Card.TLabel").grid(row=row, column=0, sticky="w", padx=(0, 10), pady=(0, 10))
        
        self.output_file_var = tk.StringVar(value=CONFIG["output_file"])
        output_file_entry = ttk.Entry(self.config_frame, textvariable=self.output_file_var, width=60)
        output_file_entry.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=(0, 10))
        
        output_button = ttk.Button(self.config_frame, text="Chọn", 
                                 command=self.select_output_file)
        output_button.grid(row=row, column=2, padx=(0, 0), pady=(0, 10))
        
        row += 1
        
        # 3. File Stocks Ratio
        ttk.Label(self.config_frame, text="File Stocks Ratio:", 
                 style="Card.TLabel").grid(row=row, column=0, sticky="w", padx=(0, 10), pady=(0, 10))
        
        self.stocks_ratio_file_var = tk.StringVar(value=CONFIG["stocks_ratio_file"])
        stocks_ratio_entry = ttk.Entry(self.config_frame, textvariable=self.stocks_ratio_file_var, width=60)
        stocks_ratio_entry.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=(0, 10))
        
        stocks_button = ttk.Button(self.config_frame, text="Chọn", 
                                 command=self.select_stocks_ratio_file)
        stocks_button.grid(row=row, column=2, padx=(0, 0), pady=(0, 10))
        
        row += 1
        
        # 4. File Total Supply EST
        ttk.Label(self.config_frame, text="File Total Supply Est:", 
                 style="Card.TLabel").grid(row=row, column=0, sticky="w", padx=(0, 10), pady=(0, 10))
        
        self.total_supply_est_file_var = tk.StringVar(value=CONFIG["total_supply_est_file"])
        est_entry = ttk.Entry(self.config_frame, textvariable=self.total_supply_est_file_var, width=60)
        est_entry.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=(0, 10))
        
        est_button = ttk.Button(self.config_frame, text="Chọn", 
                              command=self.select_total_supply_est_file)
        est_button.grid(row=row, column=2, padx=(0, 0), pady=(0, 10))
        
        row += 1
        
        # 5. File Total Supply PROJ
        ttk.Label(self.config_frame, text="File Total Supply Proj:", 
                 style="Card.TLabel").grid(row=row, column=0, sticky="w", padx=(0, 10), pady=(0, 10))
        
        self.total_supply_proj_file_var = tk.StringVar(value=CONFIG["total_supply_proj_file"])
        proj_entry = ttk.Entry(self.config_frame, textvariable=self.total_supply_proj_file_var, width=60)
        proj_entry.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=(0, 10))
        
        proj_button = ttk.Button(self.config_frame, text="Chọn", 
                               command=self.select_total_supply_proj_file)
        proj_button.grid(row=row, column=2, padx=(0, 0), pady=(0, 10))
        
        row += 1
        
        # 6. File Power BI
        ttk.Label(self.config_frame, text="File Power BI:", 
                 style="Card.TLabel").grid(row=row, column=0, sticky="w", padx=(0, 10), pady=(0, 10))
        
        self.powerbi_file_var = tk.StringVar(value=CONFIG["powerbi_file"])
        powerbi_entry = ttk.Entry(self.config_frame, textvariable=self.powerbi_file_var, width=60)
        powerbi_entry.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=(0, 10))
        
        powerbi_button = ttk.Button(self.config_frame, text="Chọn", 
                                  command=self.select_powerbi_file)
        powerbi_button.grid(row=row, column=2, padx=(0, 0), pady=(0, 10))
        
        row += 1
        
        # Checkbox để tạo file phân tích
        self.create_analysis_var = tk.BooleanVar(value=True)
        analysis_check = ttk.Checkbutton(
            self.config_frame, 
            text="Tự động tạo file phân tích sau khi xử lý (Stocks Ratio, Total Supply Est, Total Supply Proj)",
            variable=self.create_analysis_var,
            style="TCheckbutton"
        )
        analysis_check.grid(row=row, column=0, columnspan=3, sticky="w", pady=(5, 0))
    
    def create_action_section(self):
        """Tạo khu vực các nút hành động"""
        # Tiêu đề section
        title_label = ttk.Label(self.action_frame, text="Hành động", style="Title.TLabel")
        title_label.grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 10))
        
        # Tạo separator
        separator = ttk.Separator(self.action_frame, orient="horizontal")
        separator.grid(row=1, column=0, columnspan=4, sticky="ew", pady=(0, 15))
        
        # Thiết lập grid cho action_frame
        self.action_frame.columnconfigure(0, weight=1)  # Button 1
        self.action_frame.columnconfigure(1, weight=0)  # Spacer
        self.action_frame.columnconfigure(2, weight=1)  # Button 2
        self.action_frame.columnconfigure(3, weight=0)  # Additional column for alignment
        
        # Các nút hành động và mô tả
        self.normal_button = ttk.Button(
            self.action_frame,
            text="Chế độ thông thường",
            command=self.run_normal_mode,
            style="Primary.TButton"
        )
        self.normal_button.grid(row=2, column=0, sticky="ew", padx=(0, 10), pady=(0, 5))
        
        # Spacer
        ttk.Label(self.action_frame, text="", style="Card.TLabel").grid(row=2, column=1, padx=10)
        
        self.monthly_button = ttk.Button(
            self.action_frame,
            text="Cập nhật hàng tháng",
            command=self.run_monthly_mode,
            style="Danger.TButton"
        )
        self.monthly_button.grid(row=2, column=2, sticky="ew", padx=(10, 0), pady=(0, 5))
        
        # Mô tả dưới nút
        ttk.Label(
            self.action_frame,
            text="Thêm dữ liệu mới vào file hiện có",
            style="Subtitle.TLabel"
        ).grid(row=3, column=0, sticky="w", pady=(0, 15))
        
        ttk.Label(
            self.action_frame,
            text="Xóa dữ liệu cũ trước khi xử lý",
            style="Subtitle.TLabel"
        ).grid(row=3, column=2, sticky="w", pady=(0, 15))
        
        # Thanh tiến trình
        ttk.Label(
            self.action_frame,
            text="Tiến trình xử lý:",
            style="Card.TLabel"
        ).grid(row=4, column=0, sticky="w", pady=(0, 5))
        
        self.progress_var = tk.DoubleVar()
        self.progress = ttk.Progressbar(
            self.action_frame,
            orient="horizontal",
            length=100,
            mode="determinate",
            variable=self.progress_var,
            style="TProgressbar"
        )
        self.progress.grid(row=5, column=0, columnspan=3, sticky="ew", pady=(0, 10))
        
        # Label trạng thái
        self.status_var = tk.StringVar(value="Sẵn sàng")
        self.status_label = ttk.Label(
            self.action_frame,
            textvariable=self.status_var,
            style="Subtitle.TLabel"
        )
        self.status_label.grid(row=6, column=0, columnspan=3, sticky="e")
    
    def create_log_section(self):
        """Tạo khu vực hiển thị log"""
        # Tiêu đề section
        title_label = ttk.Label(self.log_frame, text="Log", style="Title.TLabel")
        title_label.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 10))
        
        # Tạo separator
        separator = ttk.Separator(self.log_frame, orient="horizontal")
        separator.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 15))
        
        # Thiết lập grid cho log_frame
        self.log_frame.columnconfigure(0, weight=1)
        self.log_frame.rowconfigure(2, weight=1)
        
        # Tạo text widget cho log
        self.log_text = scrolledtext.ScrolledText(
            self.log_frame,
            wrap=tk.WORD,
            width=80,
            height=12,
            font=("Consolas", 9),
            bg="white",
            fg=COLORS["text"],
            padx=10,
            pady=10,
            state=tk.DISABLED  # Bắt đầu với trạng thái disabled
        )
        self.log_text.grid(row=2, column=0, columnspan=2, sticky="nsew")
        
        # Cấu hình tags cho log
        self.log_text.tag_configure("info", foreground=COLORS["text"])
        self.log_text.tag_configure("warning", foreground=COLORS["warning"])
        self.log_text.tag_configure("error", foreground=COLORS["danger"])
        self.log_text.tag_configure("critical", foreground=COLORS["danger"], font=("Consolas", 9, "bold"))
        self.log_text.tag_configure("debug", foreground=COLORS["text_secondary"])
        
        # Nút Clear Log
        self.clear_log_button = ttk.Button(
            self.log_frame,
            text="Xóa Log",
            command=self.clear_log,
            style="Outline.TButton"
        )
        self.clear_log_button.grid(row=3, column=1, sticky="e", padx=0, pady=(10, 0))
    
    def create_footer(self):
        """Tạo footer với thông tin và các nút bổ sung"""
        # Copyright text
        copyright_label = ttk.Label(
            self.footer_frame,
            text="© 2024 WASDE Data Automation Tool",
            style="TLabel"
        )
        copyright_label.pack(side=tk.LEFT)
        
        # Container cho các nút footer
        footer_buttons = ttk.Frame(self.footer_frame, style="TFrame")
        footer_buttons.pack(side=tk.RIGHT)
        
        # Nút mở thư mục dữ liệu
        open_folder_button = ttk.Button(
            footer_buttons,
            text="Mở thư mục dữ liệu",
            command=self.open_data_folder,
            style="Outline.TButton"
        )
        open_folder_button.pack(side=tk.LEFT, padx=(0, 10))
        
        # Nút mở file đầu ra
        open_output_button = ttk.Button(
            footer_buttons,
            text="Mở file đầu ra",
            command=self.open_output_file,
            style="Outline.TButton"
        )
        open_output_button.pack(side=tk.LEFT)
    
    def setup_logger(self):
        """Thiết lập logger để hiển thị trong text widget"""
        # Tạo handler cho log
        log_handler = CustomLogger(self.log_text)
        log_handler.setLevel(logging.INFO)
        log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # Thêm handler vào logger root
        root_logger = logging.getLogger()
        root_logger.addHandler(log_handler)
        
        # Log thông tin khởi động
        logging.info("Ứng dụng WASDE Data Automation Tool đã khởi động")
        logging.info("Sẵn sàng xử lý dữ liệu")
    
    def select_data_folder(self):
        """Chọn thư mục dữ liệu"""
        folder = filedialog.askdirectory(initialdir=self.data_folder_var.get())
        if folder:
            self.data_folder_var.set(folder)
            CONFIG["data_folder"] = folder
            logging.info(f"Đã chọn thư mục dữ liệu: {folder}")
    
    def select_output_file(self):
        """Chọn file đầu ra"""
        file = filedialog.asksaveasfilename(
            initialdir=os.path.dirname(self.output_file_var.get()),
            initialfile=os.path.basename(self.output_file_var.get()),
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if file:
            self.output_file_var.set(file)
            CONFIG["output_file"] = file
            logging.info(f"Đã chọn file đầu ra: {file}")
    
    def select_stocks_ratio_file(self):
        """Chọn file stocks ratio"""
        file = filedialog.asksaveasfilename(
            initialdir=os.path.dirname(self.stocks_ratio_file_var.get()),
            initialfile=os.path.basename(self.stocks_ratio_file_var.get()),
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if file:
            self.stocks_ratio_file_var.set(file)
            CONFIG["stocks_ratio_file"] = file
            logging.info(f"Đã chọn file stocks ratio: {file}")
    
    def select_total_supply_est_file(self):
        """Chọn file Total Supply EST"""
        file = filedialog.asksaveasfilename(
            initialdir=os.path.dirname(self.total_supply_est_file_var.get()),
            initialfile=os.path.basename(self.total_supply_est_file_var.get()),
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if file:
            self.total_supply_est_file_var.set(file)
            CONFIG["total_supply_est_file"] = file
            logging.info(f"Đã chọn file Total Supply Est: {file}")
    
    def select_total_supply_proj_file(self):
        """Chọn file Total Supply PROJ"""
        file = filedialog.asksaveasfilename(
            initialdir=os.path.dirname(self.total_supply_proj_file_var.get()),
            initialfile=os.path.basename(self.total_supply_proj_file_var.get()),
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if file:
            self.total_supply_proj_file_var.set(file)
            CONFIG["total_supply_proj_file"] = file
            logging.info(f"Đã chọn file Total Supply Proj: {file}")
    
    def select_powerbi_file(self):
        """Chọn file Power BI"""
        file = filedialog.askopenfilename(
            initialdir=os.path.dirname(self.powerbi_file_var.get()),
            initialfile=os.path.basename(self.powerbi_file_var.get()),
            defaultextension=".pbix",
            filetypes=[("Power BI files", "*.pbix"), ("All files", "*.*")]
        )
        if file:
            self.powerbi_file_var.set(file)
            CONFIG["powerbi_file"] = file
            logging.info(f"Đã chọn file Power BI: {file}")
    
    def clear_log(self):
        """Xóa nội dung log"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
        logging.info("Đã xóa log")
    
    def open_data_folder(self):
        """Mở thư mục dữ liệu"""
        folder = self.data_folder_var.get()
        if os.path.exists(folder):
            os.startfile(folder)
            logging.info(f"Đã mở thư mục dữ liệu: {folder}")
        else:
            messagebox.showwarning("Cảnh báo", f"Thư mục không tồn tại: {folder}")
            logging.warning(f"Thư mục không tồn tại: {folder}")
    
    def open_output_file(self):
        """Mở file đầu ra"""
        file = self.output_file_var.get()
        if os.path.exists(file):
            os.startfile(file)
            logging.info(f"Đã mở file đầu ra: {file}")
        else:
            messagebox.showinfo("Thông báo", f"File chưa tồn tại: {file}")
            logging.info(f"File chưa tồn tại: {file}")
    
    def update_progress(self, value):
        """Cập nhật thanh tiến trình"""
        self.progress_var.set(value)
        self.root.update_idletasks()
    
    def update_status(self, text):
        """Cập nhật trạng thái"""
        self.status_var.set(text)
        self.root.update_idletasks()
    
    def toggle_processing_state(self, is_processing):
        """Bật/tắt trạng thái đang xử lý"""
        self.processing = is_processing
        state = tk.DISABLED if is_processing else tk.NORMAL
        self.normal_button.config(state=state)
        self.monthly_button.config(state=state)
    
    def run_normal_mode(self):
        """Chạy chế độ xử lý thông thường"""
        if self.processing:
            return
        
        # Cập nhật CONFIG từ giao diện
        CONFIG["data_folder"] = self.data_folder_var.get()
        CONFIG["output_file"] = self.output_file_var.get()
        CONFIG["stocks_ratio_file"] = self.stocks_ratio_file_var.get()
        CONFIG["total_supply_est_file"] = self.total_supply_est_file_var.get()
        CONFIG["total_supply_proj_file"] = self.total_supply_proj_file_var.get()
        CONFIG["powerbi_file"] = self.powerbi_file_var.get()
        
        # Chạy trong thread riêng để không block giao diện
        threading.Thread(target=self._run_process, args=(False,), daemon=True).start()
    
    def run_monthly_mode(self):
        """Chạy chế độ cập nhật hàng tháng"""
        if self.processing:
            return
            
        result = messagebox.askquestion(
            "Xác nhận", 
            "Chế độ này sẽ XÓA TẤT CẢ dữ liệu hiện có trong file đầu ra. Bạn có chắc chắn muốn tiếp tục?",
            icon='warning'
        )
        
        if result != 'yes':
            return
        
        # Cập nhật CONFIG từ giao diện
        CONFIG["data_folder"] = self.data_folder_var.get()
        CONFIG["output_file"] = self.output_file_var.get()
        CONFIG["stocks_ratio_file"] = self.stocks_ratio_file_var.get()
        CONFIG["total_supply_est_file"] = self.total_supply_est_file_var.get()
        CONFIG["total_supply_proj_file"] = self.total_supply_proj_file_var.get()
        CONFIG["powerbi_file"] = self.powerbi_file_var.get()
        
        # Chạy trong thread riêng để không block giao diện
        threading.Thread(target=self._run_process, args=(True,), daemon=True).start()
    
    def _run_process(self, clear_existing):
        """
        Chạy quá trình xử lý dữ liệu
        
        Args:
            clear_existing (bool): True nếu xóa dữ liệu hiện có, False nếu thêm vào
        """
        # Đánh dấu đang xử lý
        self.root.after(0, lambda: self.toggle_processing_state(True))
        self.root.after(0, lambda: self.update_progress(0))
        
        try:
            # Ghi log mode
            if clear_existing:
                self.root.after(0, lambda: self.update_status("Đang chạy chế độ cập nhật hàng tháng..."))
                logging.info("===== BẮT ĐẦU CHẾ ĐỘ CẬP NHẬT HÀNG THÁNG =====")
                
                # Xóa file filtered_data.csv nếu tồn tại
                if os.path.exists(CONFIG["output_file"]):
                    try:
                        os.remove(CONFIG["output_file"])
                        logging.info(f"Đã xóa file hiện tại: {CONFIG['output_file']}")
                    except Exception as e:
                        logging.error(f"Lỗi khi xóa file hiện tại: {str(e)}")
                
                # Xóa file stocks_ratio.csv nếu tồn tại
                if os.path.exists(CONFIG["stocks_ratio_file"]):
                    try:
                        os.remove(CONFIG["stocks_ratio_file"])
                        logging.info(f"Đã xóa file stocks ratio hiện tại: {CONFIG['stocks_ratio_file']}")
                    except Exception as e:
                        logging.error(f"Lỗi khi xóa file stocks ratio hiện tại: {str(e)}")
                
                # Xóa file Total_supply_EST.csv nếu tồn tại
                if os.path.exists(CONFIG["total_supply_est_file"]):
                    try:
                        os.remove(CONFIG["total_supply_est_file"])
                        logging.info(f"Đã xóa file Total Supply Est hiện tại: {CONFIG['total_supply_est_file']}")
                    except Exception as e:
                        logging.error(f"Lỗi khi xóa file Total Supply Est hiện tại: {str(e)}")
                
                # Xóa file Total_supply_PROJ.csv nếu tồn tại
                if os.path.exists(CONFIG["total_supply_proj_file"]):
                    try:
                        os.remove(CONFIG["total_supply_proj_file"])
                        logging.info(f"Đã xóa file Total Supply Proj hiện tại: {CONFIG['total_supply_proj_file']}")
                    except Exception as e:
                        logging.error(f"Lỗi khi xóa file Total Supply Proj hiện tại: {str(e)}")
            else:
                self.root.after(0, lambda: self.update_status("Đang chạy chế độ xử lý thông thường..."))
                logging.info("===== BẮT ĐẦU CHẾ ĐỘ XỬ LÝ THÔNG THƯỜNG =====")
            
            # Cập nhật thanh tiến trình
            self.root.after(0, lambda: self.update_progress(10))
            
            # Cập nhật thanh tiến trình
            self.root.after(0, lambda: self.update_progress(20))
            self.root.after(0, lambda: self.update_status("Đang tìm các file CSV..."))
            
            # Tìm tất cả các file CSV
            all_dataframes = []
            file_count = 0
            
            # Hàm đọc file CSV
            def read_csv_files():
                nonlocal file_count
                
                # Đọc tất cả các file CSV trong thư mục
                csv_files = []
                for root, _, files in os.walk(CONFIG["data_folder"]):
                    for file in files:
                        if file.lower().endswith('.csv') and file != os.path.basename(CONFIG["output_file"]) and file != os.path.basename(CONFIG["stocks_ratio_file"]) and file != os.path.basename(CONFIG["total_supply_est_file"]) and file != os.path.basename(CONFIG["total_supply_proj_file"]):
                            csv_files.append(os.path.join(root, file))
                
                total_files = len(csv_files)
                logging.info(f"Tìm thấy {total_files} file CSV")
                
                # Đọc từng file
                for i, file in enumerate(csv_files):
                    try:
                        # Lấy đường dẫn tương đối để hiển thị
                        relative_path = os.path.relpath(file, CONFIG["data_folder"])
                        
                        # Đọc file CSV vào DataFrame
                        df = pd.read_csv(file)
                        
                        # Thêm cột chứa tên file để dễ theo dõi nguồn dữ liệu
                        df['source_file'] = relative_path
                        
                        # Thêm cột ngày cập nhật
                        df['date_updated'] = datetime.datetime.now().strftime("%Y-%m-%d")
                        
                        # Thêm DataFrame vào danh sách
                        all_dataframes.append(df)
                        
                        # In thông tin về file đã đọc
                        logging.info(f"Đã đọc file: {relative_path}")
                        logging.info(f"Số hàng: {df.shape[0]}, Số cột: {df.shape[1]}")
                        
                        file_count += 1
                        
                        # Cập nhật tiến trình
                        progress = 20 + (i / total_files) * 30
                        self.root.after(0, lambda p=progress: self.update_progress(p))
                        self.root.after(0, lambda f=i+1, t=total_files: self.update_status(f"Đang đọc file {f}/{t}..."))
                        
                    except Exception as e:
                        logging.error(f"Lỗi khi đọc file {file}: {str(e)}")
            
            # Đọc các file CSV
            read_csv_files()
            
            # Cập nhật thanh tiến trình
            self.root.after(0, lambda: self.update_progress(50))
            self.root.after(0, lambda: self.update_status("Đang xử lý dữ liệu..."))
            
            # Xử lý dữ liệu
            if all_dataframes:
                try:
                    # Gộp tất cả các DataFrame
                    combined_df = pd.concat(all_dataframes, ignore_index=True)
                    logging.info(f"Đã gộp tất cả dữ liệu: {combined_df.shape[0]} hàng, {combined_df.shape[1]} cột")
                    
                    # Kiểm tra các cột cần thiết
                    required_columns = ['ReportTitle', 'Attribute']
                    missing_columns = [col for col in required_columns if col not in combined_df.columns]
                    
                    if missing_columns:
                        logging.warning(f"Cảnh báo: Không tìm thấy các cột sau trong dữ liệu: {missing_columns}")
                        logging.warning("Các cột hiện có: " + ", ".join(combined_df.columns.tolist()))
                        self.root.after(0, lambda: messagebox.showerror("Lỗi", f"Dữ liệu không hợp lệ! Thiếu các cột: {missing_columns}"))
                        return
                    
                    # Cập nhật thanh tiến trình
                    self.root.after(0, lambda: self.update_progress(60))
                    self.root.after(0, lambda: self.update_status("Đang lọc dữ liệu..."))
                    
                    # Lọc dữ liệu
                    filtered_df = combined_df[
                        (combined_df['ReportTitle'].isin(CONFIG["report_titles"])) & 
                        (combined_df['Attribute'].isin(CONFIG["attributes"]))
                    ]
                    
                    logging.info(f"Dữ liệu sau khi lọc: {filtered_df.shape[0]} hàng, {filtered_df.shape[1]} cột")
                    
                    # Cập nhật thanh tiến trình
                    self.root.after(0, lambda: self.update_progress(65))
                    self.root.after(0, lambda: self.update_status("Đang tạo cột True Attribute..."))
                    
                    # Tạo cột True Attribute bằng cách kết hợp Attribute và ProjEstFlag
                    if 'Attribute' in filtered_df.columns and 'ProjEstFlag' in filtered_df.columns:
                        try:
                            # Tạo cột mới True Attribute bằng cách kết hợp hai cột với khoảng trắng ở giữa
                            # Xử lý trường hợp ProjEstFlag rỗng (không thay thế bằng "nan")
                            def combine_attribute(row):
                                attribute = row['Attribute']
                                projestflag = row['ProjEstFlag']
                                
                                # Nếu ProjEstFlag có giá trị (không rỗng và không phải NaN)
                                if pd.notna(projestflag) and str(projestflag).strip() != '':
                                    return f"{attribute} {projestflag}"
                                else:
                                    return attribute  # Nếu rỗng thì chỉ giữ Attribute
                            
                            filtered_df['True Attribute'] = filtered_df.apply(combine_attribute, axis=1)
                            logging.info("Đã tạo cột True Attribute bằng cách kết hợp Attribute và ProjEstFlag.")
                        except Exception as e:
                            logging.error(f"Lỗi khi tạo cột True Attribute: {str(e)}")
                    else:
                        logging.warning("Không thể tạo cột True Attribute vì thiếu cột Attribute hoặc ProjEstFlag.")
                    
                    # Cập nhật thanh tiến trình
                    self.root.after(0, lambda: self.update_progress(70))
                    self.root.after(0, lambda: self.update_status("Đang xử lý cột ForecastYear..."))
                    
                    # Xử lý cột ForecastYear
                    if 'ForecastYear' in filtered_df.columns:
                        try:
                            # Kiểm tra kiểu dữ liệu hiện tại
                            logging.info(f"Kiểu dữ liệu của cột ForecastYear: {filtered_df['ForecastYear'].dtype}")
                            
                            # Chuyển đổi sang định dạng year (yyyy)
                            if filtered_df['ForecastYear'].dtype in ['int64', 'float64']:
                                # Đã là số, chỉ cần đảm bảo kiểu dữ liệu là int
                                filtered_df['ForecastYear'] = filtered_df['ForecastYear'].astype(int)
                                logging.info("Đã chuyển đổi ForecastYear sang định dạng số nguyên.")
                            elif filtered_df['ForecastYear'].dtype == 'object':
                                # Loại bỏ các ký tự không phải số nếu có
                                filtered_df['ForecastYear'] = filtered_df['ForecastYear'].str.extract('(\d{4})', expand=False)
                                # Chuyển sang kiểu int
                                filtered_df['ForecastYear'] = pd.to_numeric(filtered_df['ForecastYear'], errors='coerce').fillna(0).astype(int)
                                logging.info("Đã trích xuất năm từ chuỗi và chuyển đổi ForecastYear sang định dạng số nguyên.")
                            elif pd.api.types.is_datetime64_dtype(filtered_df['ForecastYear']):
                                # Chỉ lấy thành phần năm
                                filtered_df['ForecastYear'] = filtered_df['ForecastYear'].dt.year
                                logging.info("Đã trích xuất năm từ datetime và chuyển đổi ForecastYear sang định dạng số nguyên.")
                            
                            # Tạo cột mới nếu muốn giữ lại cột gốc
                            filtered_df['ForecastYearDate'] = pd.to_datetime(filtered_df['ForecastYear'], format='%Y')
                            logging.info(f"Đã chuyển đổi cột ForecastYear sang định dạng date với năm yyyy.")
                        except Exception as e:
                            logging.error(f"Lỗi khi chuyển đổi cột ForecastYear: {str(e)}")
                    
                    # Cập nhật thanh tiến trình
                    self.root.after(0, lambda: self.update_progress(80))
                    self.root.after(0, lambda: self.update_status("Đang lưu dữ liệu..."))
                    
                    # Xử lý dữ liệu hiện có
                    if os.path.exists(CONFIG["output_file"]) and not clear_existing:
                        try:
                            # Đọc dữ liệu hiện có
                            existing_df = pd.read_csv(CONFIG["output_file"])
                            logging.info(f"Đọc file hiện có với {existing_df.shape[0]} hàng")
                            
                            # Gộp với dữ liệu mới
                            final_df = pd.concat([existing_df, filtered_df], ignore_index=True)
                            logging.info(f"Gộp dữ liệu hiện có với dữ liệu mới: {final_df.shape[0]} hàng")
                            
                            # Lưu DataFrame đã lọc vào file CSV
                            final_df.to_csv(CONFIG["output_file"], index=False)
                            logging.info(f"Đã lưu dữ liệu đã lọc vào file: {CONFIG['output_file']}")
                        except Exception as e:
                            logging.error(f"Lỗi khi gộp với dữ liệu hiện có: {str(e)}")
                            # Ghi dữ liệu mới nếu không thể gộp
                            filtered_df.to_csv(CONFIG["output_file"], index=False)
                            logging.info(f"Đã lưu dữ liệu mới vào file: {CONFIG['output_file']}")
                    else:
                        # Lưu DataFrame đã lọc vào file CSV
                        filtered_df.to_csv(CONFIG["output_file"], index=False)
                        logging.info(f"Đã lưu dữ liệu đã lọc vào file: {CONFIG['output_file']}")
                    
                    # Tạo các file phân tích khác nếu cần
                    if self.create_analysis_var.get():
                        # Cập nhật thanh tiến trình
                        self.root.after(0, lambda: self.update_progress(85))
                        self.root.after(0, lambda: self.update_status("Đang tạo file Stocks Ratio..."))
                        
                        # Tạo file stocks_ratio.csv
                        success = create_stocks_ratio_csv(CONFIG["output_file"], CONFIG["stocks_ratio_file"])
                        if success:
                            logging.info(f"Đã tạo file Stocks Ratio thành công: {CONFIG['stocks_ratio_file']}")
                        else:
                            logging.error(f"Không thể tạo file Stocks Ratio")
                        
                        # Tạo file Total_supply_EST.csv
                        self.root.after(0, lambda: self.update_status("Đang tạo file Total Supply Est..."))
                        success_est = create_total_supply_est_csv(CONFIG["output_file"], CONFIG["total_supply_est_file"])
                        if success_est:
                            logging.info(f"Đã tạo file Total Supply Est thành công: {CONFIG['total_supply_est_file']}")
                        else:
                            logging.warning(f"Không thể tạo file Total Supply Est")
                        
                        # Tạo file Total_supply_PROJ.csv
                        self.root.after(0, lambda: self.update_status("Đang tạo file Total Supply Proj..."))
                        success_proj = create_total_supply_proj_csv(CONFIG["output_file"], CONFIG["total_supply_proj_file"])
                        if success_proj:
                            logging.info(f"Đã tạo file Total Supply Proj thành công: {CONFIG['total_supply_proj_file']}")
                        else:
                            logging.warning(f"Không thể tạo file Total Supply Proj")
                    
                    # Cập nhật thanh tiến trình
                    self.root.after(0, lambda: self.update_progress(90))
                    self.root.after(0, lambda: self.update_status("Đang cập nhật Power BI..."))
                    
                    # Cập nhật Power BI
                    if os.path.exists(CONFIG["powerbi_file"]):
                        try:
                            logging.info(f"Mở file Power BI: {CONFIG['powerbi_file']}")
                            os.startfile(CONFIG["powerbi_file"])
                            
                            # Đợi một khoảng thời gian để Power BI mở và tự động cập nhật
                            logging.info("Đợi Power BI cập nhật dữ liệu (30 giây)...")
                            for i in range(30):
                                time.sleep(1)
                                # Cập nhật tiến trình mỗi giây
                                progress = 90 + (i / 30) * 10
                                self.root.after(0, lambda p=progress: self.update_progress(p))
                                self.root.after(0, lambda s=30-i: self.update_status(f"Đang đợi Power BI cập nhật ({s}s)..."))
                            
                            # Gửi lệnh Alt+F4 để đóng Power BI sau khi cập nhật
                            try:
                                ps_command = '$wshell = New-Object -ComObject wscript.shell; $wshell.SendKeys("%{F4}")'
                                subprocess.run(['powershell', '-Command', ps_command], check=True)
                                logging.info("Đã đóng Power BI sau khi cập nhật")
                            except Exception as close_error:
                                logging.warning(f"Không thể tự động đóng Power BI: {str(close_error)}")
                                logging.info("Vui lòng đóng Power BI thủ công sau khi cập nhật hoàn tất")
                        except Exception as e:
                            logging.error(f"Lỗi khi cập nhật Power BI: {str(e)}")
                    else:
                        logging.warning(f"Không tìm thấy file Power BI: {CONFIG['powerbi_file']}")
                        self.root.after(0, lambda: messagebox.showwarning("Cảnh báo", f"Không tìm thấy file Power BI: {CONFIG['powerbi_file']}"))
                
                except Exception as e:
                    logging.error(f"Lỗi khi xử lý dữ liệu: {str(e)}")
                    self.root.after(0, lambda: messagebox.showerror("Lỗi", f"Lỗi khi xử lý dữ liệu: {str(e)}"))
            else:
                logging.warning("Không tìm thấy file CSV nào.")
                self.root.after(0, lambda: messagebox.showwarning("Cảnh báo", "Không tìm thấy file CSV nào để xử lý."))
            
            # Cập nhật trạng thái hoàn thành
            self.root.after(0, lambda: self.update_progress(100))
            
            # Ghi log hoàn thành
            if clear_existing:
                logging.info("===== HOÀN THÀNH CHẾ ĐỘ CẬP NHẬT HÀNG THÁNG =====")
                self.root.after(0, lambda: self.update_status("Cập nhật hàng tháng hoàn tất!"))
                self.root.after(0, lambda: messagebox.showinfo("Thành công", "Đã hoàn thành cập nhật hàng tháng!"))
            else:
                logging.info("===== HOÀN THÀNH CHẾ ĐỘ XỬ LÝ THÔNG THƯỜNG =====")
                self.root.after(0, lambda: self.update_status("Xử lý thông thường hoàn tất!"))
                self.root.after(0, lambda: messagebox.showinfo("Thành công", "Đã hoàn thành xử lý dữ liệu!"))
        
        except Exception as e:
            logging.error(f"Lỗi không mong muốn: {str(e)}")
            self.root.after(0, lambda: self.update_status("Đã xảy ra lỗi!"))
            self.root.after(0, lambda: messagebox.showerror("Lỗi", f"Lỗi không mong muốn: {str(e)}"))
        
        finally:
            # Kết thúc xử lý
            self.root.after(0, lambda: self.toggle_processing_state(False))


# Tạo hàm chạy ứng dụng với splash screen
def run_application():
    # Tạo cửa sổ chính
    root = tk.Tk()
    
    # Tạo ứng dụng
    app = ModernWasdeApp(root)
    
    # Chạy mainloop
    root.mainloop()

# Chạy ứng dụng khi script được thực thi trực tiếp
if __name__ == "__main__":
    run_application()