## Python enviroment Python 3.12.10 or later
## validate and check the dependency library
pip install pipreqs -i https://pypi.mirrors.ustc.edu.cn/simple/
pipreqs ./ --encoding=utf8 --force
## install the dependency library
pip install -r requirements.txt -i https://pypi.mirrors.ustc.edu.cn/simple/
## run directly
python main.py
## package and release the executable program in window system
pip install pyinstaller -i https://pypi.mirrors.ustc.edu.cn/simple
pyinstaller --onefile main.py
