# 概述
通过数据库的dump,分析trace文件来输出undo段的名字.

1. 通过事件 file_hdrs 找到数据库的 Root DBA Block
2. Dump Root DBA Block, 解析trace文件找到 undo$表的位置
3. Dump undo$ 段头信息 找到 Extent Map 信息
4. 遍历Extent Map信息解析Undo Segment Name

# 运行

```
perl analyzeUndoName.pl
```
