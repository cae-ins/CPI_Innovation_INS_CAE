@echo OFF
rem Define the path to your Anaconda installation
set CONDAPATH=C:\Users\dell\anaconda3
rem Define the name of the base environment
set ENVNAME=base

rem Activate the base environment
call %CONDAPATH%\Scripts\activate.bat %CONDAPATH%

rem Run a Python script in that environment
python C:\Users\dell\Desktop\DOC\PROJET_CAE_INS\Projet_CPI\script_scrapping_cpi_main.py

rem Deactivate the environment
call conda deactivate

rem Pause to keep the window open
pause
