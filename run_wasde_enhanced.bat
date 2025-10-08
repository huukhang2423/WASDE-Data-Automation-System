@echo off
echo ===== WASDE Data Automation System =====
echo.

REM Kiểm tra cài đặt Python
echo Kiểm tra cài đặt Python...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python chưa được cài đặt! Vui lòng cài đặt Python và thử lại.
    pause
    exit /b
)

REM Cài đặt các gói cần thiết
echo Đang cài đặt các gói cần thiết...
pip install pandas pillow

REM Chạy ứng dụng WASDE GUI với phiên bản nâng cao
echo Đang chạy WASDE Automation Tool...
python wasde_gui_enhanced.py

echo Nếu bạn thấy dòng này, có nghĩa là chương trình đã kết thúc.
pause
exit /b