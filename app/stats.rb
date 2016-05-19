require 'bigdecimal'

module Stats
  # copied from http://www.johndcook.com/standard_deviation.html
  class OnlineVariance
    def initialize
      @k = 0
      @m_k = 0
      @s_k = 0
      @max = nil
      @min = nil
    end

    def push_all(xs)
      xs.each {|x| push(x) }
    end

    # invariant:
    # m_k = m_kMinus1 + (x_k - m_kMinus1) / k
    # s_k = s_kMinus1 + (x_k - m_kMinus1) * (x_k - m_k)
    def push(x)
      @k += 1

      @max = @max.nil? || x > @max ? x : @max
      @min = @min.nil? || x < @min ? x : @min

      # See Knuth TAOCP vol 2, 3rd edition, page 232
      if @k == 1
        @m_k = x
        @s_k = 0
      else
        m_kPlus1 = @m_k + (x - @m_k) / @k
        s_kPlus1 = @s_k + (x - @m_k) * (x - m_kPlus1)
        @m_k = m_kPlus1
        @s_k = s_kPlus1
      end
    end

    def n
      @k
    end

    def mean
      if @k > 0
        @m_k
      else
        0
      end
    end

    def variance
      if @k > 1
        @s_k / (@k - 1)
      else
        0
      end
    end

    def standard_deviation
      Math.sqrt(variance)
    end

    def max
      @max
    end

    def min
      @min
    end
  end

  # This implementation is based on http://en.wikipedia.org/wiki/Quantiles#Estimating_the_quantiles_of_a_population
  # For additional information, see:
  # http://www.stanford.edu/class/archive/anthsci/anthsci192/anthsci192.1064/handouts/calculating%20percentiles.pdf
  # http://en.wikipedia.org/wiki/Percentile
  # http://www.mathworks.com/help/stats/quantiles-and-percentiles.html
  #
  # hFn is the function:
  #   (n: decimal) -> (p: decimal) -> decimal
  #   such that hFn returns a 1-based real-valued index (which may or may not be a whole-number) into the array of sorted values in xs
  # qSubPFn is the function:
  #   (getIthX: (int -> decimal)) -> (h: decimal) -> decimal
  #   such that getIthX returns the zero-based ith element from the array of sorted values in xs
  # percentages is a sequence of percentages expressed as real numbers in the range [0.0, 100.0]
  def self.quantiles(hFn, qSubPFn, interpolate, isSorted, percentages, xs)
    sortedXs = isSorted ? xs : xs.sort
    n = BigDecimal.new sortedXs.length    # n is the sample size
    q = BigDecimal.new 100
    p = ->(k) { k / q }
    hs = percentages.map {|percentage| hFn.call(n, p.call(percentage)) - 1 }    # NOTE: these indices are 0-based indices into sortedXs
    getIthX = ->(i) { sortedXs[i] }

    if interpolate                  # interpolate
      hs.map do |h|
        i = h.floor                 # i is a 0-based index into sortedXs   (smaller index to interpolate between)
        j = h.ceil                  # j is a 0-based index into sortedXs   (larger index to interpolate between)
        f = h - i                   # f is the fractional part of real-valued index h
        intI = i.to_i
        intJ = j.to_i
        (1 - f) * getIthX.call(intI) + f * getIthX.call(intJ)    # [1] - (1-f) * x_k + f * x_k+1 === x_k + f*(x_k+1 - x_k)
        # [1]:
        # see: http://web.stanford.edu/class/archive/anthsci/anthsci192/anthsci192.1064/handouts/calculating%20percentiles.pdf
        # also: (1-f) * x_k + f * x_k+1 === x_k - f*x_k + f*x_k+1 === x_k + f*(x_k+1 - x_k) which is what I'm after
      end
    else                            # floor the index instead of interpolating
      hs.map do |h|
        i = h.floor.to_i            # i is a 0-based index into sortedXs
        getIthX.call(i)
      end
    end
  end

  # implementation based on description of R-1 at http://en.wikipedia.org/wiki/Quantile#Estimating_the_quantiles_of_a_population
  def self.quantilesR1(interpolate, isSorted, percentages, xs)
    quantiles(
      ->(n, p) { p == 0 ? 1 : n * p + BigDecimal.new("0.5") },
      ->(getIthX, h) { getIthX.call((h - BigDecimal.new("0.5")).ceil.to_i) },
      interpolate,
      isSorted,
      percentages,
      xs
    )
  end

  # The R manual claims that "Hyndman and Fan (1996) ... recommended type 8"
  # see: http://stat.ethz.ch/R-manual/R-patched/library/stats/html/quantile.html
  # implementation based on description of R-8 at http://en.wikipedia.org/wiki/Quantile#Estimating_the_quantiles_of_a_population
  OneThird = Rational(1, 3)
  TwoThirds = Rational(2, 3)
  def self.quantilesR8(interpolate, isSorted, percentages, xs)
    quantiles(
      ->(n, p) {
        if p < TwoThirds / (n + OneThird)
          1
        elsif p >= (n - OneThird) / (n + OneThird)
          n
        else
          (n + OneThird) * p + OneThird
        end
      },
      ->(getIthX, h) {
        floorHDec = h.floor
        floorH = floorHDec.to_i
        getIthX.call(floorH) + (h - floorHDec) * (getIthX.call(floorH + 1) - getIthX.call(floorH))
      },
      interpolate,
      isSorted,
      percentages,
      xs
    )
  end

  # we use the type 8 quantile method because the R manual claims that "Hyndman and Fan (1996) ... recommended type 8"
  def self.percentiles(percentages, xs)
    quantilesR8(true, false, percentages, xs)
  end

  def self.percentiles_sorted(percentages, xs)
    quantilesR8(true, true, percentages, xs)
  end

  def self.sample_with_replacement(array, n)
    length = array.length
    n.times.map { array[rand(length)] }
  end

  def self.build_sampling_distribution(n_samples, n_observations_per_sample, build_sample_fn, compute_sample_statistic_fn)
    n_samples.times.map { compute_sample_statistic_fn.call(build_sample_fn.call(n_observations_per_sample)) }
  end

  # returns array of sampling distributions s.t. the ith sampling distribution is the sampling distribution of the statistic computed by compute_sample_statistic_fns[i]
  def self.build_sampling_distributions(n_samples, n_observations_per_sample, build_sample_fn, compute_sample_statistic_fns)
    sampling_distributions = Array.new(compute_sample_statistic_fns.count) { Array.new }
    n_samples.times do
      sample = build_sample_fn.call(n_observations_per_sample)
      compute_sample_statistic_fns.each_with_index {|compute_sample_statistic_fn, i| sampling_distributions[i] << compute_sample_statistic_fn.call(sample) }
    end
    sampling_distributions
  end

  # returns array of sampling distributions s.t. the ith sampling distribution is the sampling distribution of the statistic computed by compute_sample_statistic_fns[i]
  def self.build_sampling_distributions_from_one_multi_statistic_fn(n_samples, n_observations_per_sample, build_sample_fn, multi_statistic_fn)
    diagnostic_sample = [1,2,3]
    number_of_sampling_distributions = multi_statistic_fn.call(diagnostic_sample).count
    sampling_distributions = Array.new(number_of_sampling_distributions) { Array.new }
    n_samples.times do
      sample = build_sample_fn.call(n_observations_per_sample)
      sample_statistics = multi_statistic_fn.call(sample)
      sample_statistics.each_with_index {|sample_statistic, i| sampling_distributions[i] << sample_statistic }
    end
    sampling_distributions
  end

  def self.sample_mean(sample)
    n = sample.count
    sample.reduce(:+) / n.to_f
  end
end
