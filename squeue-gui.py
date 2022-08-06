import datetime
import getpass
import json
import os
import psutil
import sys
import subprocess
import numpy as np
import pandas as pd

# Debugging
import time

from packaging import version
from psutil._common import bytes2human
from PyQt5.QtCore import Qt, QTimer, QAbstractTableModel, QModelIndex
from PyQt5.QtWidgets import (
    QMainWindow,
    QApplication,
    QLabel,
    QPushButton,
    QCheckBox,
    QLineEdit,
    QWidget,
    QTableView,
    QMessageBox,
    # Layout
    QVBoxLayout,
    QHBoxLayout,
    QHeaderView,
    QAbstractScrollArea,
)
from PyQt5.QtGui import QIntValidator

__debug = True
SQUEUE_TIMEVAR = [
    "accrue_time",
    "eligible_time",
    "end_time",
    "last_sched_evaluation",
    "start_time",
    "submit_time",
    ]

# Model class for our table
class SqueueGUIModel(QAbstractTableModel):
    def __init__(self, array, headers, parent=None):
        QAbstractTableModel.__init__(self, parent=parent)
        self._array    = array
        self._headers  = headers
        self.r, self.c = np.shape(array)
        self._sortby   = 0
        self._sortord  = Qt.DescendingOrder

    @property
    def array(self):
        return self._array

    @property
    def headers(self):
        return self._headers

    def update_model(self, new_array, new_headers):
        self.layoutAboutToBeChanged.emit()

        old_sortvar    = self.headers[self._sortby]
        self._array    = new_array
        self._headers  = new_headers
        self.r, self.c = np.shape(new_array)

        if old_sortvar not in new_headers:
            self._sortby = 0
        else:
            self._sortby = new_headers.index(old_sortvar)
        
        # Resort the new array
        column = self._sortby
        order  = self._sortord
        argsort = self.array[:, column].argsort()
        if order == Qt.AscendingOrder:
            argsort = argsort[::-1]
        self._array = self.array[argsort]

        self.layoutChanged.emit()

    def rowCount(self, parent=QModelIndex()):
        return self.r

    def columnCount(self, parent=QModelIndex()):
        return self.c

    def headerData(self, p_int, orientation, role):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                if p_int < len(self.headers):
                    return self.headers[p_int]
            elif orientation == Qt.Vertical:
                return p_int + 1
        return

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        row = index.row()
        column = index.column()
        if row < 0 or row >= self.rowCount():
            return None
        if column < 0 or column >= self.columnCount():
            return None
        if role == Qt.DisplayRole:
            value = self.array[row, column]

            if isinstance(value, datetime.date):
                if (int(value.strftime('%s')) == 18000):
                    return 'N/A'
                else:
                    return value.isoformat()

            return value
        return None

    def setData(self, index, value, role):
        if not index.isValid():
            return False
        if role != Qt.EditRole:
            return False
        row = index.row()
        column = index.column()
        if row < 0 or row >= self.rowCount():
            return False
        if column < 0 or column >= self.columnCount():
            return False
        self.array.values[row][column] = value
        self.dataChanged.emit(index, index)
        return True

    def sort(self, column, order):
        self.layoutAboutToBeChanged.emit()
        self._sortby  = column
        self._sortord = order
        argsort = self.array[:, column].argsort()
        if order == Qt.AscendingOrder:
            argsort = argsort[::-1]
        self._array = self.array[argsort]
        self.layoutChanged.emit()

# Main window widget
class SQGUIMainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        # Check squeue main version should support squeue --json
        self.sqver = subprocess.run(
            ['squeue', '--version'],
            stdout=subprocess.PIPE,
            check=True,
            ).stdout.decode('utf-8').strip().split(" ", 2)[1]
        if version.parse(self.sqver) < version.parse("21.08"):
            self.version_error()
        
        # Set window's title
        self.setWindowTitle("squeue-gui (slurm ver. {})".format(self.sqver))
        
        # Get the user name
        self.uname = getpass.getuser()

        # Set the columns
        self.sqcols = [
            "job_id",
            "job_state",
            "user_name",
            "qos",
            "node_count",
            "cpus",
            "start_time",
            ]

        # Initialize the model
        self.sqdata  = pd.DataFrame(columns=self.sqcols)
        self.sqmodel = SqueueGUIModel(
            np.array(self.sqdata.values),
            self.sqcols,
            )
        
        # Initialize internal widgets
        self.w_sqtable = QTableView(self)
        self.w_sqtable.setSortingEnabled(True)
        self.w_sqtable.setModel(self.sqmodel)

        # Size policies for the table
        self.w_sqtable.setAlternatingRowColors(True)
        self.w_sqtable.horizontalHeader().setSectionResizeMode(
            QHeaderView.Interactive
            )
        self.w_sqtable.horizontalHeader().setStretchLastSection(True)
        self.w_sqtable.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)

        # Autorefresh timer
        self.arefcnt = 0
        self.arefrte = 1000
        self.areftmr = QTimer()
        self.areftmr.timeout.connect(self.refresh)

        # Initialize the widgets
        self.w_refresh = QPushButton("Refresh")
        self.w_refresh.setFixedWidth(170)
        self.w_refresh.clicked.connect(self.refresh)
        
        self.w_arefbut = QCheckBox("Auto refresh (s)")
        self.w_arefbut.setFixedWidth(120)
        self.w_arefrte = QLineEdit()
        self.w_arefrte.setFixedWidth(40)
        self.w_arefrte.setValidator(QIntValidator(1, 9999))
        self.w_arefrte.setText(str(1))
        
        self.w_selffil = QCheckBox("Limit to self job")
        self.w_selffil.setFixedWidth(160)

        # Connect the widgets
        self.w_arefrte.textChanged.connect(self.change_autorefresh_rate)
        self.w_arefbut.stateChanged.connect(self.toggle_autorefresh)

        # Initialize the layouts
        self.layout = QHBoxLayout()
        self.l_rghtpnl = QVBoxLayout()
        self.l_autoref = QHBoxLayout()

        # Put widgets to the layout
        self.l_autoref.addWidget(self.w_arefbut)
        self.l_autoref.addWidget(self.w_arefrte)

        self.l_rghtpnl.addWidget(self.w_refresh)
        self.l_rghtpnl.addLayout(self.l_autoref)
        self.l_rghtpnl.addWidget(self.w_selffil)
        self.l_rghtpnl.addStretch()
                
        self.layout.addWidget(self.w_sqtable)        
        self.layout.addLayout(self.l_rghtpnl)

        widget = QWidget()
        widget.setLayout(self.layout)
        self.setCentralWidget(widget)

    # Exit the program if slurm version is not high enough
    def version_error(self):
        dlg = QMessageBox(self)
        dlg.setWindowTitle("Error")
        dlg.setText(
            "slurm version {} (< 21.08) does not support JSON output".format(
                self.sqver
                )
            )
        button = dlg.exec()

        if button == QMessageBox.Ok:
            sys.exit(1)
    
    # Toggle the auto-refresh:
    def toggle_autorefresh(self):
        if self.w_arefbut.isChecked():
            self.arefcnt = 0
            self.change_autorefresh_rate(self.w_arefrte.text())
            self.areftmr.start(self.arefrte)
        else:
            self.areftmr.stop()

    # Change the auto-refresh rate
    def change_autorefresh_rate(self, new_rate):
        # Can re-implement using fixup function of QValidator
        try:
            inew_rate = int(new_rate)
            assert inew_rate > 0 and inew_rate < 10000
            self.arefrte = inew_rate * 1000
            self.areftmr.setInterval(self.arefrte)
            print('New auto-refresh rate: {} ms'.format(self.arefrte))
        except:
            print('New auto-refresh rate is invalid')
            self.w_arefrte.setText(str(1))        

    # Refresh function
    def refresh(self):
        self.arefcnt = self.arefcnt + 1

        start  = time.time()
        
        # Fetch squeue JSON
        self.sqdata = pd.DataFrame.from_dict(json.loads(
            subprocess.run(
                ['squeue', '--json'],
                stdout=subprocess.PIPE,
                check=True,
                ).stdout.decode('utf-8').strip()
            )['jobs'])
        
        # Filter the columns
        self.sqdata = self.sqdata[self.sqcols]

        # Filter for running or pending job only
        self.sqdata = self.sqdata[
            self.sqdata["job_state"].isin(['RUNNING', 'PENDING'])
            ]
        
        # Filter for own job only if needed:
        if self.w_selffil.isChecked():
            self.sqdata = self.sqdata[self.sqdata["user_name"] == self.uname]

        # Convert the datetime variables
        for col in self.sqcols:
            if col in SQUEUE_TIMEVAR:
                self.sqdata[col] = pd.to_datetime(self.sqdata[col],unit='s')
        
        # Update the model
        self.sqmodel.update_model(
            np.array(self.sqdata.values),
            self.sqcols,
            )
        self.w_sqtable.resizeColumnsToContents()

        print('Ref {:4d}, rate {}, dur {:.4f} ms, mem {}'.format(
            self.arefcnt,
            self.arefrte, 
            time.time() - start,
            bytes2human((psutil.Process(os.getpid())).memory_info().rss),
            ))


if __name__ == "__main__":
    app = QApplication(sys.argv)
    w   = SQGUIMainWindow()
    w.show()
    
    sys.exit(app.exec_())