import time
import paramiko
from config.settings import settings

def get_pihole_log_lines():
    """Obtém as últimas linhas do log do Pi-hole via SSH com sudo."""
    try:
        # Conectar via SSH
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=settings.RASPBERRY_SSH_HOST,
            username=settings.RASPBERRY_SSH_USER,
            password=settings.RASPBERRY_SSH_PASSWORD,
            timeout=10
        )
        
        # Executar comando para ler as últimas 100 linhas do log (com sudo)
        stdin, stdout, stderr = ssh.exec_command(f'sudo tail -100 {settings.PIHOLE_LOG_PATH}')
        
        # Verificar se houve erro no comando (stderr)
        error_output = stderr.read().decode('utf-8').strip()
        if error_output:
            if "Permission denied" in error_output:
                print("❌ Erro de permissão: O usuário não tem permissão para executar 'sudo tail'.")
                print("   Configure no Raspberry Pi: 'sudo visudo' e adicione:")
                print(f"   {settings.RASPBERRY_SSH_USER} ALL=(ALL) NOPASSWD: /usr/bin/tail")
                return []
            elif "No such file or directory" in error_output:
                print(f"❌ Arquivo não encontrado: {settings.PIHOLE_LOG_PATH}")
                print("   Verifique se o arquivo existe no Raspberry Pi.")
                return []
            else:
                print(f"❌ Erro no comando SSH: {error_output}")
                return []
        
        lines = stdout.read().decode('utf-8').splitlines()
        ssh.close()
        return lines
        
    except paramiko.AuthenticationException:
        print("❌ Erro de autenticação: Senha ou usuário incorretos.")
        print("   Verifique as credenciais no arquivo .env.")
        return []
    except paramiko.SSHException as e:
        print(f"❌ Erro de conexão SSH: {e}")
        print("   Verifique se o Raspberry Pi está ligado e acessível.")
        return []
    except TimeoutError:
        print("❌ Timeout: O Raspberry Pi não respondeu dentro de 10 segundos.")
        print("   Verifique a conexão de rede e o IP do Raspberry Pi.")
        return []
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return []

def parse_pihole_log_line(line):
    """Parseia uma linha do log do Pi-hole e retorna um dicionário estruturado."""
    try:
        # Exemplo: "May 12 11:46:02 dnsmasq[252714]: cached youtube-ui.l.google.com is 172.217.29.206"
        parts = line.strip().split()
        
        # Formato esperado: [Mês Dia Hora:Minuto:Segundo] [processo]: [mensagem]
        if len(parts) >= 5:
            # Extrai timestamp (May 12 11:46:02)
            timestamp = f"{parts[0]} {parts[1]} {parts[2]}"
            
            # Extrai o IP do cliente (se presente) e o domínio
            # Partes após o processo: "cached", "youtube-ui.l.google.com", "is", "172.217.29.206"
            rest = parts[3:]
            
            # O domínio geralmente é o segundo item após o timestamp
            if len(rest) >= 2:
                # Remove o ":" do final do processo
                process = rest[0].replace(':', '')
                domain_or_ip = rest[1] if len(rest) > 1 else ''
                
                return {
                    'timestamp': timestamp,
                    'process': process,
                    'domain': domain_or_ip.replace(':', ''),
                    'raw': line.strip(),
                    'parsed': True
                }
            else:
                return {
                    'timestamp': timestamp,
                    'raw': line.strip(),
                    'parsed': False
                }
        else:
            return {'raw': line.strip(), 'parsed': False}
    except Exception as e:
        return {'raw': line.strip(), 'error': str(e), 'parsed': False}

def test_log_reader():
    """Testa a leitura do log do Pi-hole e imprime na tela."""
    print(f"🔍 Conectando a {settings.RASPBERRY_SSH_HOST} via SSH...")
    print(f"📂 Lendo {settings.PIHOLE_LOG_PATH}\n")
    
    lines = get_pihole_log_lines()
    
    if not lines:
        print("\n✅ Diagnóstico: Nenhuma linha foi lida. Verifique as mensagens acima.")
        return
    
    print(f"📊 Total de linhas lidas: {len(lines)}\n")
    print("=" * 60)
    print("📋 Últimas 20 linhas do log do Pi-hole:")
    print("=" * 60)
    
    # Mostra as últimas 20 linhas
    for i, line in enumerate(lines[-20:], 1):
        parsed = parse_pihole_log_line(line)
        print(f"\n[{i}] Raw: {line}")
        print(f"    Parsed: {parsed}")
    
    print("\n" + "=" * 60)
    print("✅ Teste concluído!")

if __name__ == '__main__':
    test_log_reader()
