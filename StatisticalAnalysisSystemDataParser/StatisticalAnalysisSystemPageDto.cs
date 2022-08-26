namespace StatisticalAnalysisSystemDataParser
{
    public class StatisticalAnalysisSystemPageDto
    {
        public int PageNumber { get; set; }
        public byte[] PageData { get; set; }
        public int PageType { get; set; }
        public int SubHeaderCount { get; set; }
    }
}
