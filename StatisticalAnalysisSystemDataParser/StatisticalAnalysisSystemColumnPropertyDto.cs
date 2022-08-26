namespace StatisticalAnalysisSystemDataParser
{
    public class StatisticalAnalysisSystemColumnPropertyDto
    {
        public string Name { get; set; }
        public int AttributeOffset { get; set; }
        public int AttributeLength { get; set; }
        public string DataType { get; set; }
        public string Format { get; set; }
        public int FormatIndex { get; set; }
        public int FormatOffset { get; set; }
        public int FormatLength { get; set; }
        public string Label { get; set; }
        public int LabelIndex { get; set; }
        public int LabelOffset { get; set; }
        public int LabelLength { get; set; }
        public int ColumnIndex { get; set; }
    }
}
