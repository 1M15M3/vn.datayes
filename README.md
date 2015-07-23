# vn.past-demo
基于通联数据接口和MongoDB的历史数据子模块，从属于vnpy：https://github.com/vnpy/vnpy


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

* http://nbviewer.ipython.org/github/zedyang/vn.past-demo/blob/master/Tutorial.ipynb


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

#### 2.2   首次使用
Demo中目前加载了使用通联数据Api下载股票日线数据和期货合约日线数据的方法。

首次使用时：

1. 用文本编辑器打开base.py，填写通联数据的用户token。

2. 执行init.py初始化MongoDB数据库。即下载全部股票与期货合约日线数据并储存至MongoDB。默认的初始化数据跨度为：股票从2013年1月1日至2015年7月20日；期货合约从2015年1月1日至2015年7月20日。各使用最大30个CPU线程。根据网速的不同，下载会花费大概8到15分钟。

---

### 3. Methods

#### 3.1 fetch
* DataGenerator.fetch( ticker, start, end, field=-1, output='list' )
    * ticker: 字符串, 股票或期货合约代码。
    * start, end: ‘yyyymmdd’ 格式字符串；查询时间起止点。
    * field: 字符串列表，所选取的key。默认值为－1，选取所有key。
    * output: 字符串，输出格式。默认为'list'，输出字典列表。可选的类型为：
        * 'list'： 输出字典列表。
        * 'df'： 输出pandas DataFrame。
        * 'bar'： 输出本模块内建的Bar数据结构，为一个包含日期，开、收、高、低价格以及成交量的DataFrame，之后详细介绍。注意若选择输出bar，则参数field的值会被忽略。
        
#### 3.2 update
* DataGenerator.update( )

    * 从数据库中获取存在的最新日期，然后自动更新数据库到今日。
    * 根据网速的不同，更新一到五个交易日所需时间为1分钟到200秒不等。
    
#### 3.3 绘图相关

* Bar.get_candlist( )

    * 我们知道matplotlib.finance.candlestick_ochl要求严格的input形式。为[(t,o,c,h,l),...]这样的数组列表。
    * 内建Bar DataFrame加入了一个方法自己形成这种格式输出，方便作K线图。
    

* Resampler.rspfbar_date(rate)

    * 对Bar数据进行再取样。rate＝取样率。
    * 仍在测试中。
    
    
---