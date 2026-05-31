import tkinter as tk
from tkinter import ttk
import subprocess
import time

class ServiceControl:
    def __init__(self, root):
        self.root = root
        self.root.title("Kafka n APIs Control Panel")
        self.root.geometry("700x500")
        
        # Lista de serviços (exceto o primeiro produtor do Raspberry Pi)
        self.services = [
            # Producers
            "producer_2_api_logs",
            "producer_3_api_ideas",
            "producer_4_public_apis",
            "producer_5_faker",
            # Consumers
            "consumer_1_logs",
            "consumer_2_api_logs",
            "consumer_3_api_ideas",
            "consumer_4_public_apis",
            "consumer_5_faker"
        ]
        
        self.status = {}
        self.create_widgets()
        self.refresh_status()
    
    def create_widgets(self):
        # Frame para a lista
        frame = ttk.Frame(self.root, padding=10)
        frame.pack(fill="both", expand=True)
        
        # Cabeçalhos
        ttk.Label(frame, text="Service", font=("Arial", 10, "bold")).grid(row=0, column=0, padx=5, pady=5, sticky="w")
        ttk.Label(frame, text="Status", font=("Arial", 10, "bold")).grid(row=0, column=1, padx=5, pady=5, sticky="w")
        ttk.Label(frame, text="Actions", font=("Arial", 10, "bold")).grid(row=0, column=2, padx=5, pady=5, sticky="w")
        
        # Linhas para cada serviço
        self.rows = {}
        for i, svc in enumerate(self.services, 1):
            # Nome
            name = ttk.Label(frame, text=svc, font=("Arial", 9))
            name.grid(row=i, column=0, padx=5, pady=3, sticky="w")
            
            # Status
            status_label = ttk.Label(frame, text="Loading...", font=("Arial", 9))
            status_label.grid(row=i, column=1, padx=5, pady=3, sticky="w")
            
            # Botões
            btn_start = ttk.Button(frame, text="▶ Start", command=lambda s=svc: self.action(s, "start"))
            btn_start.grid(row=i, column=2, padx=2, pady=3)
            
            btn_stop = ttk.Button(frame, text="⏹ Stop", command=lambda s=svc: self.action(s, "stop"))
            btn_stop.grid(row=i, column=3, padx=2, pady=3)
            
            btn_restart = ttk.Button(frame, text="🔄 Restart", command=lambda s=svc: self.action(s, "restart"))
            btn_restart.grid(row=i, column=4, padx=2, pady=3)
            
            self.rows[svc] = {
                "status": status_label,
                "start": btn_start,
                "stop": btn_stop,
                "restart": btn_restart
            }
        
        # Botão de atualizar
        ttk.Button(frame, text="🔄 Refresh", command=self.refresh_status).grid(row=len(self.services)+1, column=0, columnspan=5, pady=10)
    
    def get_status(self, service):
        """Retorna o status de um serviço no Windows."""
        try:
            if service.startswith("producer"):
                # Para produtores (processos Python)
                # Procura pelo nome do módulo na lista de processos
                result = subprocess.run(
                    f'tasklist /fi "imagename eq python.exe" /v | findstr "{service}"',
                    shell=True, capture_output=True, text=True
                )
                return "Running" if result.stdout else "Stopped"
            else:
                # Para consumidores (serviços Windows)
                result = subprocess.run(f'sc query {service}', shell=True, capture_output=True, text=True)
                if "RUNNING" in result.stdout:
                    return "Running"
                return "Stopped"
        except:
            return "Unknown"
    
    def refresh_status(self):
        """Atualiza o status de todos os serviços."""
        for svc in self.services:
            status = self.get_status(svc)
            self.status[svc] = status
            self.rows[svc]["status"].config(text=status)
    
    def action(self, service, action):
        """Executa uma ação (start, stop, restart) no serviço."""
        try:
            if service.startswith("producer"):
                if action == "start":
                    # Inicia o produtor em uma nova janela
                    subprocess.Popen(
                        f'start cmd /k "python -m src.producers.{service}"',
                        shell=True
                    )
                elif action == "stop":
                    # Para o processo
                    subprocess.run(
                        f'taskkill /f /im python.exe /fi "windowtitle eq {service}"',
                        shell=True
                    )
                elif action == "restart":
                    self.action(service, "stop")
                    time.sleep(1)
                    self.action(service, "start")
            else:
                # Para consumidores (serviços Windows)
                subprocess.run(f'sc {action} {service}', shell=True)
            self.refresh_status()
        except Exception as e:
            tk.messagebox.showerror("Error", str(e))

if __name__ == "__main__":
    root = tk.Tk()
    app = ServiceControl(root)
    root.mainloop()