using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace StatisticalAnalysisSystemDataParser
{
    public static class StatisticalAnalysisSystemDataParser
    {
        private const int PageTypeMeta = 0;
        //private static int PageTypeData = 256;
        private const int PageTypeMix = 512;
        private const int PageTypeAmd = 1024;
        private const int PageTypeMetaCompressed = 16384;
        private const int PageTypeComp = -28672;

        private static readonly byte[] RowSizeSignature = { 0xF7, 0xF7, 0xF7, 0xF7 };
        private static readonly byte[] ColumnSizeSignature = { 0xF6, 0xF6, 0xF6, 0xF6 };
        private static readonly byte[] ColumnTextSignature = { 0xFD, 0xFF, 0xFF, 0xFF };
        private static readonly byte[] ColumnAttributeSignature = { 0xFC, 0xFF, 0xFF, 0xFF };
        private static readonly byte[] ColumnNameSignature = { 0xFF, 0xFF, 0xFF, 0xFF };
        private static readonly byte[] ColumnFormatSignature = { 0xFE, 0xFB, 0xFF, 0xFF };

        private const string AttributeTypeNumeric = "numeric";
        private const string AttributeTypeCharacter = "character";

        private const string ColumnFormatComma = "COMMA";
        private const string ColumnFormatDate = "YYMMDD";

        public static DataTable ParseData(byte[] fileContent, string tableName = "")
        {
            var dataTable = new DataTable(tableName);

            var alignmentByte = fileContent[32];

            var isUnix64 = alignmentByte.Equals(0x33);

            var alignmentByte2 = fileContent[35];

            var alignmentValue2 = alignmentByte2.Equals(0x33) ? 4 : 0;

            var endianByte = fileContent[37];

            if (!endianByte.Equals(0x01))
            {
                throw new Exception("Big endian files are not supported!");
            }

            var headerLength = fileContent.GetInteger(196 + alignmentValue2, 4);

            var pageSize = fileContent.GetInteger(200 + alignmentValue2, 4);

            var pageCount = fileContent.GetInteger(204 + alignmentValue2, 4);

            var dataPages = new List<StatisticalAnalysisSystemPageDto>();

            for (var i = 1; i <= pageCount; i++)
            {
                var pageBuffer = fileContent.Skip(headerLength).Take(pageSize * i).ToArray();

                var dataPage = new StatisticalAnalysisSystemPageDto
                {
                    PageData = pageBuffer,
                    PageNumber = 1,
                    SubHeaderCount = pageBuffer.GetInteger(isUnix64 ? 36 : 20, 2),
                    PageType = pageBuffer.GetInteger(isUnix64 ? 32 : 16, 2)
                };

                dataPages.Add(dataPage);
            }

            var subHeaderList = new List<StatisticalAnalysisSystemSubHeaderDto>();

            foreach (var page in dataPages)
            {
                subHeaderList.AddRange(ReadSubHeaders(page, isUnix64));
            }

            var rowSizeSubHeader = subHeaderList.FirstOrDefault(s => s.Signature.SequenceEqual(RowSizeSignature));

            if (rowSizeSubHeader == null)
                throw new Exception("Row size subheader not found!");

            var rowLength = rowSizeSubHeader.SubHeaderData.GetInteger(isUnix64 ? 40 : 20, isUnix64 ? 8 : 4);

            var rowCount = rowSizeSubHeader.SubHeaderData.GetInteger(isUnix64 ? 48 : 24, isUnix64 ? 8 : 4);

            var rowCountFirstPage = rowSizeSubHeader.SubHeaderData.GetInteger(isUnix64 ? 120 : 60, isUnix64 ? 8 : 4);

            var columnSizeSubHeader = subHeaderList.FirstOrDefault(s => s.Signature.SequenceEqual(ColumnSizeSignature));

            if (columnSizeSubHeader == null)
                throw new Exception("Column size subheader not found!");

            var columnCount = columnSizeSubHeader.SubHeaderData.GetInteger(isUnix64 ? 8 : 4, isUnix64 ? 8 : 4);

            var columnTextSubHeaderList =
                subHeaderList.Where(s => s.Signature.SequenceEqual(ColumnTextSignature)).ToList();

            if (!columnTextSubHeaderList.Any())
            {
                throw new Exception("Column information subheader not found!");
            }

            var columnAttributeSubHeaderList =
                subHeaderList.Where(s => s.Signature.SequenceEqual(ColumnAttributeSignature)).ToList();

            if (!columnAttributeSubHeaderList.Any())
            {
                throw new Exception("Column attribute subheader not found!");
            }

            var columnPropertyList = ReadColumnAttributes(columnAttributeSubHeaderList, isUnix64);

            if (columnPropertyList.Count != columnCount)
                throw new Exception("Column attributes for all columns not found");

            var columnNameSubHeaderList =
                subHeaderList.Where(s => s.Signature.SequenceEqual(ColumnNameSignature)).ToList();

            if (!columnNameSubHeaderList.Any())
                throw new Exception("Column name subheaders not found!");

            columnPropertyList = ReadColumnNames(columnPropertyList, columnNameSubHeaderList, columnTextSubHeaderList,
                isUnix64);

            //Should make column names unique? Is there such a scenario?
            var columnFormatSubHeaderList =
                subHeaderList.Where(s => s.Signature.SequenceEqual(ColumnFormatSignature)).ToList();

            columnPropertyList =
                ReadColumnFormatList(columnPropertyList, columnFormatSubHeaderList, columnTextSubHeaderList, isUnix64);

            // Column list is columnNameList, columnAttributeList and columnFormatList

            if (dataPages.Any(page => new[] { PageTypeComp, PageTypeMetaCompressed }.Contains(page.PageType)))
            {
                throw new Exception("File contains compressed page that can not be read!");
            }

            foreach (var columnProperty in columnPropertyList)
            {
                var type = typeof(string);

                if (columnProperty.DataType.Equals(AttributeTypeNumeric))
                {
                    switch (columnProperty.Format)
                    {
                        case ColumnFormatComma:
                            type = typeof(decimal);
                            break;
                        case ColumnFormatDate:
                            type = typeof(DateTime);
                            break;
                        default:
                            type = typeof(decimal);
                            break;
                    }
                }

                dataTable.Columns.Add(columnProperty.Name, type);
            }

            foreach (var page in dataPages)
            {
                var baseIndex = (isUnix64 ? 32 : 16) + 8;
                int pageRowCount;

                if (new[] { PageTypeMix }.Contains(page.PageType))
                {
                    pageRowCount = rowCountFirstPage;
                    baseIndex += page.SubHeaderCount * (isUnix64 ? 24 : 12);
                    baseIndex += baseIndex % 8;
                }
                else
                {
                    pageRowCount = page.PageData.GetInteger(isUnix64 ? 34 : 18, 2);
                }

                baseIndex = (baseIndex + 7) / 8 * 8 + baseIndex % 8;

                if (pageRowCount > rowCount)
                    pageRowCount = rowCount;

                for (var i = 1; i <= pageRowCount; i++)
                {
                    var dataRow = dataTable.NewRow();
                    //for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
                    foreach (var columnProperty in columnPropertyList)
                    {
                        var offset = baseIndex + columnProperty.AttributeOffset;
                        if (columnProperty.AttributeLength <= 0)
                            continue;

                        if (columnProperty.DataType == AttributeTypeNumeric)
                        {
                            switch (columnProperty.Format)
                            {
                                case ColumnFormatDate:
                                    dataRow[columnProperty.Name] = page.PageData
                                        .GetDouble(offset, columnProperty.AttributeLength).ConvertFromSASDateTime();
                                    break;
                                case ColumnFormatComma:
                                    dataRow[columnProperty.Name] =
                                        page.PageData.GetDouble(offset, columnProperty.AttributeLength);
                                    break;
                                default:
                                    dataRow[columnProperty.Name] =
                                        page.PageData.GetDouble(offset, columnProperty.AttributeLength);
                                    break;
                            }
                        }
                        else
                        {
                            dataRow[columnProperty.Name] =
                                page.PageData.GetString(offset, columnProperty.AttributeLength);
                        }
                    }

                    dataTable.Rows.Add(dataRow);
                    baseIndex += rowLength;
                }
            }

            return dataTable;
        }

        private static List<StatisticalAnalysisSystemSubHeaderDto> ReadSubHeaders(StatisticalAnalysisSystemPageDto page,
            bool isUnix64)
        {
            var subHeaderList = new List<StatisticalAnalysisSystemSubHeaderDto>();

            if (!new[]
                {
                    PageTypeMeta, PageTypeMix, PageTypeAmd, PageTypeMetaCompressed
                }.Contains(page.PageType))
                return subHeaderList;

            var subHeaderOffsetPointer = isUnix64 ? 40 : 24;
            var subHeaderPointerLength = isUnix64 ? 24 : 12;
            var firstFieldSubHeaderLength = isUnix64 ? 8 : 4;

            for (var i = 1; i <= page.SubHeaderCount; i++)
            {
                var baseIndex = subHeaderOffsetPointer + (i - 1) * subHeaderPointerLength;
                var subHeader = new StatisticalAnalysisSystemSubHeaderDto
                {
                    Length = page.PageData.GetInteger(baseIndex + firstFieldSubHeaderLength, firstFieldSubHeaderLength),
                    Offset = page.PageData.GetInteger(baseIndex, firstFieldSubHeaderLength)
                };

                if (subHeader.Length > 0)
                {
                    subHeader.SubHeaderData = new byte[subHeader.Length];
                    subHeader.Signature = new byte[4];

                    subHeader.SubHeaderData = page.PageData.Skip(subHeader.Offset).Take(subHeader.Length).ToArray();
                    subHeader.Signature = subHeader.SubHeaderData.Take(4).ToArray();
                }

                if (subHeader.Signature != null) //Exclude subheaders without signature
                    subHeaderList.Add(subHeader);
            }

            return subHeaderList;
        }

        private static List<StatisticalAnalysisSystemColumnPropertyDto> ReadColumnAttributes(
            List<StatisticalAnalysisSystemSubHeaderDto> subHeaderList, bool isUnix64)
        {
            var columnPropertyList = new List<StatisticalAnalysisSystemColumnPropertyDto>();

            var columnAttributeVector = isUnix64 ? 16 : 12;

            foreach (var subHeader in subHeaderList)
            {
                var attributeCount = (subHeader.Length - (isUnix64 ? 28 : 20)) / columnAttributeVector;

                for (var i = 0; i < attributeCount; i++)
                {
                    var baseIndex = columnAttributeVector + i * columnAttributeVector;

                    var columnProperty = new StatisticalAnalysisSystemColumnPropertyDto
                    {
                        AttributeOffset = subHeader.SubHeaderData.GetInteger(baseIndex, isUnix64 ? 8 : 4),
                        AttributeLength = subHeader.SubHeaderData.GetInteger(baseIndex + (isUnix64 ? 8 : 4), 4),
                        DataType = subHeader.SubHeaderData.GetInteger(baseIndex + (isUnix64 ? 14 : 10), 1) == 1
                            ? AttributeTypeNumeric
                            : AttributeTypeCharacter,
                        ColumnIndex = i
                    };

                    columnPropertyList.Add(columnProperty);
                }
            }

            return columnPropertyList;
        }

        private static List<StatisticalAnalysisSystemColumnPropertyDto> ReadColumnNames(
            List<StatisticalAnalysisSystemColumnPropertyDto> columnPropertyList,
            List<StatisticalAnalysisSystemSubHeaderDto> subHeaderList,
            List<StatisticalAnalysisSystemSubHeaderDto> columnInformationSubHeaderList, bool isUnix64)
        {
            var offsetBase = isUnix64 ? 8 : 4;

            foreach (var subHeader in subHeaderList)
            {
                var columnCount = (subHeader.Length - (isUnix64 ? 28 : 20)) / 8;
                for (var i = 0; i < columnCount; i++)
                {
                    var columnProperty = columnPropertyList.FirstOrDefault(cp => cp.ColumnIndex == i);
                    if (columnProperty == null)
                        throw new Exception($"Column property for index {i} not found!");

                    var baseIndex = (isUnix64 ? 16 : 12) + i * 8;
                    var headerIndex = subHeader.SubHeaderData.GetInteger(baseIndex, 2);
                    var offset = subHeader.SubHeaderData.GetInteger(baseIndex + 2, 2);
                    var length = subHeader.SubHeaderData.GetInteger(baseIndex + 4, 2);

                    columnProperty.Name = columnInformationSubHeaderList[headerIndex].SubHeaderData
                        .GetString(offsetBase + offset + headerIndex, length);
                }
            }

            return columnPropertyList;
        }

        private static List<StatisticalAnalysisSystemColumnPropertyDto> ReadColumnFormatList(
            List<StatisticalAnalysisSystemColumnPropertyDto> columnPropertyList,
            List<StatisticalAnalysisSystemSubHeaderDto> columnFormatSubHeaderList,
            List<StatisticalAnalysisSystemSubHeaderDto> columnInformationSubHeaderList, bool isUnix64)
        {
            if (columnFormatSubHeaderList.Count < 1)
            {
                return columnPropertyList;
            }

            var baseOffset = isUnix64 ? 8 : 4;

            var columnIndex = 0;
            foreach (var columnFormatSubHeader in columnFormatSubHeaderList)
            {
                var columnProperty = columnPropertyList.FirstOrDefault(cp => cp.ColumnIndex == columnIndex);

                if (columnProperty == null)
                    throw new Exception($"Column property at index {columnIndex} not found!");

                var formatBaseIndex = isUnix64 ? 46 : 34;
                var labelBaseIndex = isUnix64 ? 52 : 40;

                columnProperty.FormatIndex = columnFormatSubHeader.SubHeaderData.GetInteger(formatBaseIndex, 2);
                columnProperty.FormatOffset = columnFormatSubHeader.SubHeaderData.GetInteger(formatBaseIndex + 2, 2);
                columnProperty.FormatLength = columnFormatSubHeader.SubHeaderData.GetInteger(formatBaseIndex + 4, 2);
                columnProperty.LabelIndex = columnFormatSubHeader.SubHeaderData.GetInteger(labelBaseIndex, 2);
                columnProperty.LabelOffset = columnFormatSubHeader.SubHeaderData.GetInteger(labelBaseIndex + 2, 2);
                columnProperty.LabelLength = columnFormatSubHeader.SubHeaderData.GetInteger(labelBaseIndex + 4, 2);

                if (columnProperty.FormatLength > 0)
                {
                    columnProperty.Format = columnInformationSubHeaderList[columnProperty.FormatIndex].SubHeaderData
                        .GetString(baseOffset + columnProperty.FormatOffset, columnProperty.FormatLength);
                }

                if (columnProperty.LabelLength > 0)
                {
                    columnProperty.Label = columnInformationSubHeaderList[columnProperty.LabelIndex].SubHeaderData
                        .GetString(baseOffset + columnProperty.LabelOffset, columnProperty.FormatLength);
                }

                columnIndex++;
            }

            return columnPropertyList;
        }
    }
}