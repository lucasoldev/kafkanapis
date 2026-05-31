import sys
import subprocess
import time
import qtawesome as qta
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QTableWidget, QTableWidgetItem, QPushButton,
                             QHeaderView, QLabel, QMessageBox)
from PyQt6.QtCore import QTimer, Qt

class ServiceControl(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Kafka n APIs Control Panel")
        self.setGeometry(100, 100, 800, 500)
        
        # Lista de serviços
        self.services = [
            ("producer_2_api_logs", "Producer"),
            ("producer_3_api_ideas", "Producer"),
            ("producer_4_public_apis", "Producer"),
            ("producer_5_faker", "Producer"),
            ("consumer_1_logs", "Consumer"),
            ("consumer_2_api_logs", "Consumer"),
            ("consumer_3_api_ideas", "Consumer"),
            ("consumer_4_public_apis", "Consumer"),
            ("consumer_5_faker", "Consumer")
        ]
        
        self.initUI()
        self.refresh_status()
        
        # Auto-refresh a cada 5 segundos
        self.timer = QTimer()
        self.timer.timeout.connect(self.refresh_status)
        self.timer.start(5000)
    
    def initUI(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout()
        
        # Tabela
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["Service", "Type", "Status", "Actions"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.table)
        
        # Botão de refresh manual
        refresh_btn = QPushButton("Refresh")
        refresh_btn.setIcon(qta.icon('fa5s.sync-alt'))
        refresh_btn.clicked.connect(self.refresh_status)
        layout.addWidget(refresh_btn)
        
        central_widget.setLayout(layout)
    
    def get_status(self, service):
        """Retorna o status de um serviço."""
        try:
            if service.startswith("producer"):
                result = subprocess.run(
                    f'tasklist /fi "imagename eq python.exe" /v | findstr "{service}"',
                    shell=True, capture_output=True, text=True
                )
                return "Running" if result.stdout else "Stopped"
            else:
                result = subprocess.run(f'sc query {service}', shell=True, capture_output=True, text=True)
                if "RUNNING" in result.stdout:
                    return "Running"
                return "Stopped"
        except:
            return "Unknown"
    
    def refresh_status(self):
        self.table.setRowCount(len(self.services))
        for i, (service, service_type) in enumerate(self.services):
            # Nome
            self.table.setItem(i, 0, QTableWidgetItem(service))
            # Tipo
            self.table.setItem(i, 1, QTableWidgetItem(service_type))
            # Status
            status = self.get_status(service)
            status_item = QTableWidgetItem(status)
            if status == "Running":
                status_item.setBackground(Qt.GlobalColor.green)
            elif status == "Stopped":
                status_item.setBackground(Qt.GlobalColor.red)
            self.table.setItem(i, 2, status_item)
            
            # Botões
            btn_widget = QWidget()
            btn_layout = QHBoxLayout()
            btn_layout.setContentsMargins(0, 0, 0, 0)
            
            btn_start = QPushButton()
            btn_start.setIcon(qta.icon('fa5s.play'))
            btn_start.setToolTip("Start")
            btn_start.clicked.connect(lambda _, s=service: self.action(s, "start"))
            
            btn_stop = QPushButton()
            btn_stop.setIcon(qta.icon('fa5s.stop'))
            btn_stop.setToolTip("Stop")
            btn_stop.clicked.connect(lambda _, s=service: self.action(s, "stop"))
            
            btn_restart = QPushButton()
            btn_restart.setIcon(qta.icon('fa5s.sync-alt'))
            btn_restart.setToolTip("Restart")
            btn_restart.clicked.connect(lambda _, s=service: self.action(s, "restart"))
            
            btn_layout.addWidget(btn_start)
            btn_layout.addWidget(btn_stop)
            btn_layout.addWidget(btn_restart)
            btn_widget.setLayout(btn_layout)
            self.table.setCellWidget(i, 3, btn_widget)
    
    def action(self, service, action):
        try:
            if action == "start":
                subprocess.Popen(
                    f'start cmd /k "python -m src.{service}"',
                    shell=True
                )
            elif action == "stop":
                subprocess.run(
                    f'taskkill /f /im python.exe /fi "windowtitle eq {service}"',
                    shell=True
                )
            elif action == "restart":
                self.action(service, "stop")
                time.sleep(1)
                self.action(service, "start")
            self.refresh_status()
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ServiceControl()
    window.show()
    sys.exit(app.exec())