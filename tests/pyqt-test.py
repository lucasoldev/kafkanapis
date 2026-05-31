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
        
        # Caminho do Python dentro do venv
        self.python_path = r"X:\Bibliotecas\Imagens\Git_profissa\kafkanapis\venv\Scripts\python.exe"
        
        # Mapeamento: nome_amigável -> nome_do_módulo
        self.services = {
            "producer_2_api_logs": "src.producers.producer_2_api_logs",
            "producer_3_api_ideas": "src.producers.producer_3_api_ideas",
            "producer_4_public_apis": "src.producers.producer_4_public_apis",
            "producer_5_faker": "src.producers.producer_5_faker",
            "consumer_1_logs": "src.consumers.consumer_1_logs",
            "consumer_2_api_logs": "src.consumers.consumer_2_api_logs",
            "consumer_3_api_ideas": "src.consumers.consumer_3_api_ideas",
            "consumer_4_public_apis": "src.consumers.consumer_4_public_apis",
            "consumer_5_faker": "src.consumers.consumer_5_faker"
        }
        
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
    
    def get_status(self, service_module):
        """Retorna o status de um serviço."""
        try:
            result = subprocess.run(
                f'tasklist /fi "imagename eq python.exe" /v | findstr "{service_module}"',
                shell=True, capture_output=True, text=True
            )
            return "Running" if result.stdout else "Stopped"
        except:
            return "Unknown"
    
    def refresh_status(self):
        self.table.setRowCount(len(self.services))
        for i, (display_name, service_module) in enumerate(self.services.items()):
            # Nome amigável
            self.table.setItem(i, 0, QTableWidgetItem(display_name))
            # Tipo (producer/consumer)
            service_type = "Producer" if "producer" in service_module else "Consumer"
            self.table.setItem(i, 1, QTableWidgetItem(service_type))
            # Status
            status = self.get_status(service_module)
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
            btn_start.clicked.connect(lambda _, m=service_module: self.action(m, "start"))
            
            btn_stop = QPushButton()
            btn_stop.setIcon(qta.icon('fa5s.stop'))
            btn_stop.setToolTip("Stop")
            btn_stop.clicked.connect(lambda _, m=service_module: self.action(m, "stop"))
            
            btn_restart = QPushButton()
            btn_restart.setIcon(qta.icon('fa5s.sync-alt'))
            btn_restart.setToolTip("Restart")
            btn_restart.clicked.connect(lambda _, m=service_module: self.action(m, "restart"))
            
            btn_layout.addWidget(btn_start)
            btn_layout.addWidget(btn_stop)
            btn_layout.addWidget(btn_restart)
            btn_widget.setLayout(btn_layout)
            self.table.setCellWidget(i, 3, btn_widget)
    
    def action(self, service_module, action):
        try:
            if action == "start":
                subprocess.Popen(
                    f'start cmd /k "{self.python_path} -m {service_module}"',
                    shell=True
                )
            elif action == "stop":
                subprocess.run(
                    f'taskkill /f /im python.exe /fi "windowtitle eq {service_module}"',
                    shell=True
                )
            elif action == "restart":
                self.action(service_module, "stop")
                time.sleep(1)
                self.action(service_module, "start")
            self.refresh_status()
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ServiceControl()
    window.show()
    sys.exit(app.exec())