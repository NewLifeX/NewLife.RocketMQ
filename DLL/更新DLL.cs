// 自动选择最新的文件源
var di = ".".AsDirectory();
var srcs = new String[] { @"..\Bin", @"C:\X\DLL", @"C:\X\Bin", @"D:\X\Bin", @"D:\X\Bin\net45", @"..\..\..\X\Bin\net45" };
di.CopyIfNewer(srcs, "*.dll;*.exe;*.xml;*.pdb;*.cs");

srcs = new String[] { @"..\..\BI\Bin", @"D:\ZTO\BI\Bin", @"E:\ZTO\BI\Bin", @"..\..\BI.Core\Bin", @"..\..\BI.Core\DLL" };
di.CopyIfNewer(srcs, "*.dll;*.exe;*.xml;*.pdb;*.cs");

di = "..\\DLL20".AsDirectory();
if (di.Exists)
{
    srcs = new String[] { @"D:\X\Bin\netstandard2.0", @"..\..\..\X\Bin\netstandard2.0" };
    di.CopyIfNewer(srcs, "*.dll;*.exe;*.xml;*.pdb;*.cs");

    srcs = new String[] { @"..\..\BI.Core\Bin\netstandard2.0", @"..\..\BI.Core\DLL20" };
    di.CopyIfNewer(srcs, "*.dll;*.exe;*.xml;*.pdb;*.cs");
}