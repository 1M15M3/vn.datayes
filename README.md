# vn.past-demo
基于通联数据接口和MongoDB的历史数据子模块，从属于vnpy：https://github.com/vnpy/vnpy

## ！项目重构中，请不要下载当前repo ！

---

### 1. Preface
past 是一个从属于vn.trader的市场历史数据解决方案模块。主要功能为：

* 从datayes（通联数据）等web数据源高效地爬取、更新历史数据。
* 基于MongoDB的数据库管理、快速查询，各种输出格式的转换。
* 基于Matplotlib快速绘制K线图等可视化对象。

**主要依赖**：pymongo，pandas，requests，json

**开发测试环境**：

* OS X 10.10 / Windows 7
* Anaconda.Python 2.7

**Tutorial**:

* http://nbviewer.ipython.org/github/zedyang/vn.past-demo/blob/master/Tutorial_demo.ipynb


---

### 2. Get Started

#### 2.1   使用前

安装MongoDB: https://www.mongodb.org/downloads


更新pymongo至3.0以上版本：


	~$ pip install pymongo --upgrade
    
    
安装或更新requests：

    ~$ pip install requests --upgrade
    
启动MongoDB：

    ~$ mongod

