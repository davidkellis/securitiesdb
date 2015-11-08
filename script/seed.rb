require_relative '../application'

def run
  Application.load_config(ARGV.first || Application::DEFAULT_CONFIG_FILE_PATH)

  # Strategy.create(name: Strategy::Names::BuyAndHold)
  #
  # TrialSetDistributionType.create(name: TrialSetDistributionType::Names::WeeklyNonOverlappingTrials)
  #
  # SampleStatistic.create(name: "mean")
  # SampleStatistic.create(name: "min")
  # SampleStatistic.create(name: "max")
  # SampleStatistic.create(name: "percentile1")
  # SampleStatistic.create(name: "percentile5")
  # SampleStatistic.create(name: "percentile10")
  # SampleStatistic.create(name: "percentile15")
  # SampleStatistic.create(name: "percentile20")
  # SampleStatistic.create(name: "percentile25")
  # SampleStatistic.create(name: "percentile30")
  # SampleStatistic.create(name: "percentile35")
  # SampleStatistic.create(name: "percentile40")
  # SampleStatistic.create(name: "percentile45")
  # SampleStatistic.create(name: "percentile50")
  # SampleStatistic.create(name: "percentile55")
  # SampleStatistic.create(name: "percentile60")
  # SampleStatistic.create(name: "percentile65")
  # SampleStatistic.create(name: "percentile70")
  # SampleStatistic.create(name: "percentile75")
  # SampleStatistic.create(name: "percentile80")
  # SampleStatistic.create(name: "percentile85")
  # SampleStatistic.create(name: "percentile90")
  # SampleStatistic.create(name: "percentile95")
  # SampleStatistic.create(name: "percentile99")
end

def main
  run
end

main if __FILE__ == $0
