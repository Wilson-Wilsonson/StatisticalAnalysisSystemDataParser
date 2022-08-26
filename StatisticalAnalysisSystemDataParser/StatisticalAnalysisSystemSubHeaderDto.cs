namespace StatisticalAnalysisSystemDataParser
{
    public class StatisticalAnalysisSystemSubHeaderDto
    {
        public int Offset { get; set; }
        public int Length { get; set; }
        public byte[] SubHeaderData { get; set; }
        public byte[] Signature { get; set; }
    }
}
