import sys
import json
import logging
from PyQt5.QtCore import Qt, pyqtSlot, QThread, pyqtSignal
from PyQt5.QtWidgets import (
  QApplication,
  QMainWindow,
  QPushButton,
  QWidget,
  QLineEdit,
  QFormLayout,
  QComboBox,
  QProgressBar,
  QMessageBox,
  QRadioButton,
)
import mysql.connector as mysql
import pyodbc

# Configurar loggers
info_logger = logging.getLogger("info_logger")
info_logger.setLevel(logging.INFO)
info_handler = logging.FileHandler("info.log", encoding="utf-8")
info_handler.setLevel(logging.INFO)
info_formatter = logging.Formatter("%(asctime)s | %(message)s", datefmt="%d-%m %H:%M")
info_handler.setFormatter(info_formatter)
info_logger.addHandler(info_handler)

error_logger = logging.getLogger("error_logger")
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler("error.log", encoding="utf-8")
error_handler.setLevel(logging.ERROR)
error_formatter = logging.Formatter(
  "%(asctime)s | %(message)s", datefmt="%d-%m %H:%M:%S"
)
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)

# Função para substituir caracteres
def substituir_caracteres(new_char, chars):
  for char, subst in chars.items():
    new_char = new_char.replace(char, subst)
  return new_char


class DatabaseThread(QThread):
  progress = pyqtSignal(int)
  finished = pyqtSignal()
  error = pyqtSignal(str)

  def __init__(self, conn, db_type, tabela, coluna, chars, parent=None):
    super().__init__(parent)
    self.conn = conn
    self.db_type = db_type
    self.tabela = tabela
    self.coluna = coluna
    self.chars = chars

  # Função para selecionar as tabelas e colunas do servidor
  def run(self):
    try:
      cursor = self.conn.cursor()
      if self.db_type == "mysql":
        cursor.execute(f"SELECT {self.coluna} FROM {self.tabela}")
      else:
        cursor.execute(f"SELECT RTRIM({self.coluna}) FROM {self.tabela}")

      rows = cursor.fetchall()
      total_tasks = len(rows)
      current_task = 0

      for row in rows:
        texto_antigo = row[0]
        if texto_antigo:
          texto_novo = substituir_caracteres(texto_antigo, self.chars)
          info_logger.info(f"{texto_antigo} ==> {texto_novo}\n")
          if self.db_type == "mysql":
            cursor.execute(
              f"UPDATE {self.tabela} SET {self.coluna} = %s WHERE {self.coluna} = %s",
              (texto_novo, texto_antigo),
            )
          else:
            cursor.execute(
              f"UPDATE {self.tabela} SET {self.coluna} = ? WHERE {self.coluna} = ?",
              (texto_novo, texto_antigo),
            )

      current_task += 1
      self.progress.emit(int(current_task / total_tasks * 100))

      self.conn.commit()
      cursor.close()
      self.finished.emit()
    except Exception as e:
      error_logger.error(str(e))
      self.error.emit(str(e))


class MainWindow(QMainWindow):
  def __init__(self):
    super().__init__()
    self.setWindowTitle("App")
    self.setFixedSize(400, 300)

    self.load_config()

    self.host_input = QLineEdit(self)
    self.port_input = QLineEdit(self)
    self.user_input = QLineEdit(self)
    self.password_input = QLineEdit(self)
    self.database_input = QLineEdit(self)

    self.mysql_radio = QRadioButton("MySQL", self)
    self.sqlserver_radio = QRadioButton("SQL Server", self)
    self.mysql_radio.setChecked(True)  # MySQL como padrão
    self.mysql_radio.toggled.connect(self.load_mysql_config)
    self.sqlserver_radio.toggled.connect(self.load_sqlserver_config)

    self.tabelas_combo = QComboBox(self)
    self.colunas_combo = QComboBox(self)

    self.connect_button = QPushButton("Conectar", self)
    self.connect_button.clicked.connect(self.connect_to_database)

    self.start_button = QPushButton("Iniciar Substituição", self)
    self.start_button.setEnabled(False)
    self.start_button.clicked.connect(self.start_process)

    self.progress = QProgressBar(self)
    self.progress.setAlignment(Qt.AlignCenter)

    form_layout = QFormLayout()
    form_layout.addRow("Host:", self.host_input)
    form_layout.addRow("Port:", self.port_input)
    form_layout.addRow("User:", self.user_input)
    form_layout.addRow("Password:", self.password_input)
    self.password_input.setEchoMode(QLineEdit.Password)
    form_layout.addRow("Database:", self.database_input)
    form_layout.addRow(self.mysql_radio)
    form_layout.addRow(self.sqlserver_radio)
    form_layout.addRow(self.connect_button)
    form_layout.addRow("Tabelas:", self.tabelas_combo)
    form_layout.addRow("Colunas:", self.colunas_combo)
    form_layout.addRow(self.start_button)
    form_layout.addRow(self.progress)

    container = QWidget()
    container.setLayout(form_layout)
    self.setCentralWidget(container)

    self.conn = None
    self.load_mysql_config()

  # Função para carregar as informações do arquivo
  def load_config(self):
    try:
      with open("config.json", encoding="utf-8") as file:
        configuracoes = json.load(file)
        self.config = configuracoes["credenciais"]
        self.chars = configuracoes["caracteres"]
    except Exception as e:
      QMessageBox.critical(self, "Erro", f"Erro ao carregar configurações: {e}")
      error_logger.error(f"Erro ao carregar configurações: {e}")
      self.config = {}
      self.chars = {}

  def load_mysql_config(self):
    mysql_config = self.config.get("mysql", {})
    self.host_input.setText(mysql_config.get("host", ""))
    self.port_input.setText(mysql_config.get("port", ""))
    self.user_input.setText(mysql_config.get("user", ""))
    self.password_input.setText(mysql_config.get("password", ""))
    self.database_input.setText(mysql_config.get("database", ""))

  def load_sqlserver_config(self):
    sqlserver_config = self.config.get("sqlserver", {})
    self.host_input.setText(sqlserver_config.get("host", ""))
    self.port_input.setText(sqlserver_config.get("port", ""))
    self.user_input.setText(sqlserver_config.get("user", ""))
    self.password_input.setText(sqlserver_config.get("password", ""))
    self.database_input.setText(sqlserver_config.get("database", ""))

  @pyqtSlot()
  def connect_to_database(self):
    credenciais = {
      "host": self.host_input.text(),
      "port": self.port_input.text(),
      "user": self.user_input.text(),
      "password": self.password_input.text(),
      "database": self.database_input.text(),
    }
    try:
      if self.mysql_radio.isChecked():
        self.conn = mysql.connect(
          host=credenciais["host"],
          port=credenciais["port"],
          user=credenciais["user"],
          password=credenciais["password"],
          database=credenciais["database"],
          charset=credenciais.get("charset", "utf8"),
          )
      else:
        self.conn = pyodbc.connect(
          f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={credenciais['host']},{credenciais['port']};DATABASE={credenciais['database']};UID={credenciais['user']};PWD={credenciais['password']}"
      )
      cursor = self.conn.cursor()
      if self.mysql_radio.isChecked():
        cursor.execute("SHOW TABLES")
      else:
        cursor.execute(
          "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME ASC"
      )

      tables = cursor.fetchall()
      self.tabelas_combo.clear()
      for table in tables:
        self.tabelas_combo.addItem(table[0])
      self.tabelas_combo.currentIndexChanged.connect(self.load_columns)
      cursor.close()
      self.start_button.setEnabled(True)
      QMessageBox.information(self, "Sucesso", "Conexão bem-sucedida!")

    except (mysql.Error, pyodbc.Error) as e:
      QMessageBox.critical(
        self, "Erro", f"Erro ao conectar ao banco de dados: {e}"
      )
      error_logger.error(f"Erro ao conectar ao banco de dados: {e}")
      self.start_button.setEnabled(False)

  # Função para carregar as colunas
  def load_columns(self):
    tabela = self.tabelas_combo.currentText()
    if not tabela:
      return
    try:
      cursor = self.conn.cursor()
      if self.mysql_radio.isChecked():
        cursor.execute(f"SHOW COLUMNS FROM {tabela}")
      else:
        cursor.execute(
          f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tabela}'"
        )

      columns = cursor.fetchall()
      self.colunas_combo.clear()
      for column in columns:
        self.colunas_combo.addItem(column[0])
      cursor.close()
    except (mysql.Error, pyodbc.Error) as e:
      QMessageBox.critical(self, "Erro", f"Erro ao carregar colunas: {e}")
      error_logger.error(f"Erro ao carregar colunas: {e}")

  @pyqtSlot()
  def start_process(self):
    tabela = self.tabelas_combo.currentText()
    coluna = self.colunas_combo.currentText()
    db_type = "mysql" if self.mysql_radio.isChecked() else "sqlserver"

    self.thread = DatabaseThread(self.conn, db_type, tabela, coluna, self.chars)
    self.thread.progress.connect(self.update_progress)
    self.thread.finished.connect(self.process_finished)
    self.thread.error.connect(self.process_error)
    self.thread.start()

  @pyqtSlot(int)
  def update_progress(self, value):
    self.progress.setValue(value)

  @pyqtSlot()
  def process_finished(self):
    QMessageBox.information(
      self, "Concluído", "Substituição de caracteres concluída com sucesso."
    )
    self.progress.setValue(0)

  @pyqtSlot(str)
  def process_error(self, error_message):
    QMessageBox.critical(self, "Erro", f"Ocorreu um erro: {error_message}")
    self.progress.setValue(0)


if __name__ == "__main__":
  app = QApplication(sys.argv)
  window = MainWindow()
  window.show()
  sys.exit(app.exec_())
