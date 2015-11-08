# def fact(i); i <= 1 ? 1 : i * fact(i - 1); end
# pp (1..64).map{|i| (1..32).map{|j| [i, j, 2**i, fact(j), 2**i - fact(j)] } }.flatten(1).select{|t| t[2] > t[3] }.sort_by{|t| t[0].to_f / t[1] } ; nil

require 'stringio'
# require 'rbzip2'

class BitWriter
  def initialize
    @pos = 0
    @bytes_written = 0
    @current_byte = 0
    @bit_string = ""
    @io = StringIO.new(@bit_string)
  end
  
  # write the unsigned integer value, int, in n bits to the bit-string
  # WARNING: if int is negative, then the signed representation is probably not what you want to be written to the bitstring
  def write(int, n)
    raise "Ruby doesn't play nice with negative number bit manipulations!" if int < 0
    if n > 0
      @next_pos = @pos + n
      
      until remaining_bits_to_write == 0
        number_of_bits_to_write = if remaining_bits_to_write > number_of_free_bits_in_current_byte
          # then, we need to write to all of the remaining free bits in the current byte
          number_of_free_bits_in_current_byte
        else
          # write the remaining bits of <int> to a portion of the current byte
          remaining_bits_to_write
        end
        
        rightmost_bits_mask = (1 << number_of_bits_to_write) - 1
        rightshift_count = remaining_bits_to_write - number_of_bits_to_write
        @current_byte = (@current_byte << number_of_bits_to_write) | (unsigned_right_shift(int, rightshift_count) & rightmost_bits_mask)
        # puts int
        # print_byte(@current_byte)
        # puts
        @pos += number_of_bits_to_write
        
        write_byte if at_beginning_of_byte_boundary?    # if we're at the beginning of a byte-boundary, we need to write the current byte to the bitstring and create a new byte-buffer
      end
    end
  end
  
  def close
    write(0, number_of_free_bits_in_current_byte) unless at_beginning_of_byte_boundary?   # pad the tail-end of the current byte with zeros if the byte is only partially written
    @io.close
  end
  
  def to_s
    @bit_string
  end
  
  private
  
  def unsigned_right_shift(int, n)
    # attempt 1 - doen't work becaue ruby keeps promoting everything to Bignum
    # bits = int.size * 8
    # shift_amount = bits - n
    # mask = ~((-1 >> shift_amount) << shift_amount)
    # (int >> n) && mask
    
    # attempt 2 - doen't work becaue ruby keeps promoting everything to Bignum
    # int = int < 0 ? -int : int
    # int >> n
    
    # attempt 3 - arithmetic right shift works so long as the number isn't negative
    int >> n
  end
  
  def write_byte
    @io.putc(@current_byte)
    @current_byte = 0
    @bytes_written += 1
  end
  
  def at_beginning_of_byte_boundary?
    current_byte_bit_position == 0
  end
  
  def current_byte_bit_position
    @pos % 8
  end
  
  def number_of_free_bits_in_current_byte
    8 - current_byte_bit_position
  end
  
  def remaining_bits_to_write
    @next_pos - @pos
  end
end

class BitReader
  def initialize(bit_string_io)
    @pos = 0
    @bytes_read = 0
    @current_byte = nil
    @bit_string_byte_enum = bit_string_io.each_byte
  end
  
  # read n bits from the bit-string as an Integer value
  #   n should be a reasonable integer size (probably < 128 bits)
  # returns an unsigned integer representing the n-bit string, or nil if there aren't any more bits to read
  def read(n)
    sum = 0
    if n > 0
      @next_pos = @pos + n
      
      until remaining_bits_to_read == 0
        if at_beginning_of_byte_boundary?     # if we're at the beginning of a byte-boundary, we need to read the "current byte" into memory
          @current_byte = read_byte
          return nil if @current_byte.nil?
        end
        number_of_bits_to_read = if remaining_bits_to_read > number_of_unread_bits_in_current_byte
          # then, read all the unread bits in the current byte
          number_of_unread_bits_in_current_byte
        else
          # read just a portion of the current byte
          remaining_bits_to_read
        end
        sum = (sum << number_of_bits_to_read) | Byte.extract_int_lr(@current_byte, current_byte_bit_position, current_byte_bit_position + number_of_bits_to_read - 1)
        @pos += number_of_bits_to_read
      end
    end
    sum
  end
  
  private
  
  # returns an integer byte or nil if we've reached the end of the bitstring
  def read_byte
    byte = @bit_string_byte_enum.next
    @bytes_read += 1
    byte
  rescue StopIteration => e   # @bit_string_byte_enum.next blew up => we're out of bytes to consume
    nil
  end
  
  def at_beginning_of_byte_boundary?
    current_byte_bit_position == 0
  end
  
  def current_byte_bit_position
    @pos % 8
  end
  
  def number_of_unread_bits_in_current_byte
    8 - current_byte_bit_position
  end
  
  def remaining_bits_to_read
    @next_pos - @pos
  end
end


#############################################################################################################################################################################
########################################################################## ENCODER LOGIC ####################################################################################
#############################################################################################################################################################################

def factorial(i)
  i <= 1 ? 1 : i * factorial(i - 1)
end

class GlobalFrameOfReferenceIntListEncoder
  def encode(ints)
    return "" if ints.empty?
    
    bw = BitWriter.new

    ints = delta_encode(ints.sort)
    signed_start_int = ints.first
    remaining_ints = ints.drop(1)   # these are the int differences from the delta-encoded int list (i.e. all but the first int in the original list)
    
    number_of_ints = remaining_ints.count
    signed_min = number_of_ints > 0 ? remaining_ints.min : 0
    remaining_ints = remaining_ints.map{|int| int - signed_min }   # after this line, all ints in remaining_ints are guaranteed to be non-negative
    int_bit_size = number_of_ints > 0 ? bit_length(remaining_ints.max) : 0
    
    VariableByteIntEncoder.write_signed(bw, signed_start_int)
    VariableByteIntEncoder.write_signed(bw, signed_min)
    VariableByteIntEncoder.write(bw, number_of_ints)
    VariableByteIntEncoder.write(bw, int_bit_size)
    
    remaining_ints.each do |int|
      bw.write(int, int_bit_size)
    end
    
    bw.close
    
    bw.to_s
  end
  
  def decode(bin_str)
    return [] if (bin_str.respond_to?(:empty?) && bin_str.empty?) || (bin_str.respond_to?(:eof?) && bin_str.eof?)
    
    br = BitReader.new(bin_str)
    
    signed_start_int = VariableByteIntEncoder.read_signed(br)
    signed_min_int = VariableByteIntEncoder.read_signed(br)
    number_of_ints = VariableByteIntEncoder.read(br)
    int_bit_size = VariableByteIntEncoder.read(br)
    
    delta_encoded_int_list = [signed_start_int]
    number_of_ints.times do
      delta_encoded_int_list << signed_min_int + br.read(int_bit_size)
    end
    
    delta_decode(delta_encoded_int_list)
  end
end

# Performance of BinaryPackingIntListEncoder
# See http://lemire.me/blog/archives/2012/02/08/effective-compression-using-frame-of-reference-and-delta-coding/
# See http://arxiv.org/pdf/1209.2137.pdf for comparison of state-of-the-art integer compression techniques
# ================================================================================
# 2^15 uniformly distributed ints in the range [0, 536870912) ; (Uniform: Short arrays)
# --------------------------------------------------------------------------------
# Average bit count per int
# 28.01025390625
# --------------------------------------------------------------------------------
# BinaryPackingIntListEncoder block=128
# 16.826904296875 bits per int
# 0.6007408698683866 compressed/uncompressed ratio
# pass
# 
# ================================================================================
# 2^15 normally distributed floats (mu = 268435456, sigma = 33554432) ; (ClusterData: Short arrays)
# --------------------------------------------------------------------------------
# Average bit count per int
# 28.50396728515625
# --------------------------------------------------------------------------------
# BinaryPackingIntListEncoder block=128
# 14.952392578125 bits per int
# 0.5245723315824748 compressed/uncompressed ratio
# pass
# 
# ================================================================================
# 2^25 uniformly distributed ints in the range [0, 536870912) ; (Uniform: Long arrays)
# --------------------------------------------------------------------------------
# Average bit count per int
# 27.999861270189285
# --------------------------------------------------------------------------------
# BinaryPackingIntListEncoder block=128
# 7.05992317199707 bits per int
# 0.2521413625543061 compressed/uncompressed ratio
# pass
# 
# ================================================================================
# 2^25 normally distributed floats (mu = 268435456, sigma = 33554432) ; (ClusterData: Long arrays)
# --------------------------------------------------------------------------------
# Average bit count per int
# 28.500020265579224
# --------------------------------------------------------------------------------
# BinaryPackingIntListEncoder block=128
# 5.202009677886963 bits per int
# 0.1825265255747789 compressed/uncompressed ratio
# pass
class BinaryPackingIntListEncoder
  def initialize(block_size = 128)
    @block_size = block_size
  end
  
  def encode(ints, sort_list = true)
    return "" if ints.empty?
    
    bw = BitWriter.new

    ints = delta_encode(sort_list ? ints.sort : ints)
    
    signed_start_int = ints.first
    remaining_ints = ints.drop(1)   # these are the int differences from the delta-encoded int list (i.e. all but the first int in the original list)
    slices = remaining_ints.each_slice(@block_size).to_a
    number_of_slices = slices.count
    
    VariableByteIntEncoder.write_signed(bw, signed_start_int)
    VariableByteIntEncoder.write(bw, number_of_slices)
    
    slices.each do |slice_of_ints|
      FrameOfReferenceIntListEncoder.write(bw, slice_of_ints)
    end
    
    bw.close
    
    bw.to_s
  end
  
  def decode(bin_str)
    return [] if (bin_str.respond_to?(:empty?) && bin_str.empty?) || (bin_str.respond_to?(:eof?) && bin_str.eof?)
    
    br = BitReader.new(bin_str)
    
    signed_start_int = VariableByteIntEncoder.read_signed(br)
    number_of_slices = VariableByteIntEncoder.read(br)
    
    delta_encoded_int_list = [signed_start_int]
    number_of_slices.times do
      delta_encoded_int_list.concat(FrameOfReferenceIntListEncoder.read(br))
    end
    
    delta_decode(delta_encoded_int_list)
  end
end

# an attempt to use the SortedFrameOfReferenceIntListEncoder2
class BinaryPackingIntListEncoder2
  def initialize(block_size = 128)
    @block_size = block_size
  end
  
  def encode(ints, sort_list = true)
    return "" if ints.empty?
    
    bw = BitWriter.new

    ints = delta_encode(sort_list ? ints.sort : ints)
    
    signed_start_int = ints.first
    remaining_ints = ints.drop(1)   # these are the int differences from the delta-encoded int list (i.e. all but the first int in the original list)
    slices = remaining_ints.each_slice(@block_size).to_a
    number_of_slices = slices.count
    
    VariableByteIntEncoder.write_signed(bw, signed_start_int)
    VariableByteIntEncoder.write(bw, number_of_slices)
    
    slices.each do |slice_of_ints|
      SortedFrameOfReferenceIntListEncoder2.write(bw, slice_of_ints)
    end
    
    bw.close
    
    bw.to_s
  end
  
  def decode(bin_str)
    return [] if (bin_str.respond_to?(:empty?) && bin_str.empty?) || (bin_str.respond_to?(:eof?) && bin_str.eof?)
    
    br = BitReader.new(bin_str)
    
    signed_start_int = VariableByteIntEncoder.read_signed(br)
    number_of_slices = VariableByteIntEncoder.read(br)
    
    delta_encoded_int_list = [signed_start_int]
    number_of_slices.times do
      delta_encoded_int_list.concat(SortedFrameOfReferenceIntListEncoder2.read(br))
    end
    
    delta_decode(delta_encoded_int_list)
  end
end

class Bzip2BinaryPackingIntListEncoder
  def initialize(block_size = 128)
    @encoder = BinaryPackingIntListEncoder.new(block_size)
  end
  
  def encode(ints, sort_list = true)
    encoded_ints_as_string = @encoder.encode(ints, sort_list)
    strio = StringIO.new()
    bz2 = RBzip2::Compressor.new strio  # wrap the file into the compressor
    bz2.write encoded_ints_as_string    # write the raw data to the compressor
    bz2.close                           # finish compression (important!)
    strio.string
  end
  
  def decode(bin_str)
    strio = StringIO.new(bin_str)
    bz2 = RBzip2::Decompressor.new strio  # wrap the file into the decompressor
    unzipped_bin_str = bz2.read           # read data into a string
    decoded_ints = @encoder.decode(unzipped_bin_str)
  end
end

class DoubleEncodedBinaryPackingIntListEncoder
  def initialize(block_size = 128)
    @encoder = BinaryPackingIntListEncoder.new(block_size)
  end
  
  def encode(ints, sort_list = true)
    encoded_ints_as_string = @encoder.encode(ints, sort_list)
    encoded_byte_array = @encoder.encode(encoded_ints_as_string.each_byte.to_a, false)
  end
  
  def decode(bin_str)
    decoded_byte_array = @encoder.decode(bin_str)
    decoded_ints = @encoder.decode(decoded_byte_array.pack("c*"))
  end
end

class SortedFrameOfReferenceIntListEncoder
  SLICE_SIZE = 5
  BITS_NEEDED_TO_REPRESENT_SORT_ORDER = 7   # 7 since 5!=120 < 2^7=128
  # SLICE_SIZE = 12
  # BITS_NEEDED_TO_REPRESENT_SORT_ORDER = 29   #29 since 12! < 2^29
  # SLICE_SIZE = 9
  # BITS_NEEDED_TO_REPRESENT_SORT_ORDER = 19
  
  # ints is an array of signed integers
  def self.write(bw, ints)
    number_of_ints = ints.count
    if number_of_ints > 0
      slices = ints.each_slice(SLICE_SIZE).to_a
      number_of_slices = slices.count
    
      VariableByteIntEncoder.write(bw, number_of_ints)
      VariableByteIntEncoder.write(bw, number_of_slices)
      
      # process all but the last slice
      (0...(slices.length - 1)).each do |slice_index|
        slice_of_ints = slices[slice_index]
        sorted_slice_of_ints, ordering = *ArrayReordering.sort_and_identify_ordering(slice_of_ints)
        sort_order = identify_sort_order_inverse(ordering)

        delta_encoded_ints = delta_encode(sorted_slice_of_ints)
        signed_start_int = delta_encoded_ints.first
        remaining_ints = delta_encoded_ints.drop(1)   # these are the 4 int differences from the delta-encoded int list
    
        VariableByteIntEncoder.write_signed(bw, signed_start_int)
        
        bw.write(sort_order, BITS_NEEDED_TO_REPRESENT_SORT_ORDER)

        FixedCountFrameOfReferenceIntListEncoder.write(bw, remaining_ints, SLICE_SIZE - 1)
      end
      
      # process the last slice
      slice_of_ints = slices.last
      number_of_zeros = SLICE_SIZE - slice_of_ints.count
      slice_of_ints.concat(number_of_zeros.times.map{0})    # append a number of zeros to make sure the list is <SLICE_SIZE> elements in length
      sorted_slice_of_ints, ordering = *ArrayReordering.sort_and_identify_ordering(slice_of_ints)
      sort_order = identify_sort_order_inverse(ordering)

      delta_encoded_ints = delta_encode(sorted_slice_of_ints)
      signed_start_int = delta_encoded_ints.first
      remaining_ints = delta_encoded_ints.drop(1)   # these are the 4 int differences from the delta-encoded int list
  
      VariableByteIntEncoder.write_signed(bw, signed_start_int)
      
      bw.write(sort_order, BITS_NEEDED_TO_REPRESENT_SORT_ORDER)

      FixedCountFrameOfReferenceIntListEncoder.write(bw, remaining_ints, SLICE_SIZE - 1)
    end
  end
  
  # ORDERING_TO_SORT_ORDER is of the form:
  # {[[0, 0], [1, 1], [2, 2], [3, 3], [4, 4]]=>1,
  #  [[0, 0], [1, 1], [2, 2], [4, 3], [3, 4]]=>2,
  #  [[0, 0], [1, 1], [3, 2], [2, 3], [4, 4]]=>3,
  #  [[0, 0], [1, 1], [4, 2], [2, 3], [3, 4]]=>4,
  #  [[0, 0], [1, 1], [3, 2], [4, 3], [2, 4]]=>5,
  #  ... }
  # ORDERING_TO_SORT_ORDER = [0,1,2,3,4].permutation.to_a.map{|triple| [0,1,2,3,4].zip(triple).sort_by{|pair| pair.last } }.zip(1..120).to_h
  ORDERING_TO_SORT_ORDER = (0...SLICE_SIZE).to_a.permutation.to_a.map{|triple| (0...SLICE_SIZE).to_a.zip(triple).sort_by{|pair| pair.last } }.zip(1..factorial(SLICE_SIZE)).to_h
  
  # SORT_ORDER_TO_ORDERING is of the form:
  # {1=>[[0, 0], [1, 1], [2, 2], [3, 3], [4, 4]],
  #  2=>[[0, 0], [1, 1], [2, 2], [4, 3], [3, 4]],
  #  3=>[[0, 0], [1, 1], [3, 2], [2, 3], [4, 4]],
  #  4=>[[0, 0], [1, 1], [4, 2], [2, 3], [3, 4]],
  #  5=>[[0, 0], [1, 1], [3, 2], [4, 3], [2, 4]],
  #  ... }
  SORT_ORDER_TO_ORDERING = ORDERING_TO_SORT_ORDER.invert
  
  def self.identify_sort_order(ordering)
    ORDERING_TO_SORT_ORDER[ordering.sort_by{|pair| pair.last }]
  end
  
  def self.identify_sort_order_inverse(ordering)
    ORDERING_TO_SORT_ORDER[ArrayReordering.inverse_ordering(ordering).sort_by{|pair| pair.last }]
  end

  # returns an array of signed integers
  def self.read(br)
    number_of_ints = VariableByteIntEncoder.read(br)
    number_of_slices = VariableByteIntEncoder.read(br)
    number_of_zeros = number_of_slices * SLICE_SIZE - number_of_ints
    
    ints = []
    
    # process all but the last slice
    (number_of_slices - 1).times do
      signed_start_int = VariableByteIntEncoder.read_signed(br)
      sort_order = br.read(BITS_NEEDED_TO_REPRESENT_SORT_ORDER)
      ordering = identify_ordering(sort_order)
      
      delta_encoded_ints = FixedCountFrameOfReferenceIntListEncoder.read(br, SLICE_SIZE - 1)
      delta_encoded_ints.unshift(signed_start_int)
      
      sorted_ints = delta_decode(delta_encoded_ints)
      
      reordered_ints = ArrayReordering.reorder(sorted_ints, ordering)
      
      ints.concat(reordered_ints)
    end
    
    # process the last slice
    signed_start_int = VariableByteIntEncoder.read_signed(br)
    sort_order = br.read(BITS_NEEDED_TO_REPRESENT_SORT_ORDER)
    ordering = identify_ordering(sort_order)
    
    delta_encoded_ints = FixedCountFrameOfReferenceIntListEncoder.read(br, SLICE_SIZE - 1)
    delta_encoded_ints.unshift(signed_start_int)
    
    sorted_ints = delta_decode(delta_encoded_ints)
    
    reordered_ints = ArrayReordering.reorder(sorted_ints, ordering)
    reordered_ints = reordered_ints.take(SLICE_SIZE - number_of_zeros)
    
    ints.concat(reordered_ints)
    
    
    ints
  end
  
  def self.identify_ordering(sort_order)
    SORT_ORDER_TO_ORDERING[sort_order]
  end
end

class BlockEncoder
  def self.write(bw, ints, block_size)
    slices = ints.each_slice(block_size).to_a
    number_of_slices = slices.count
    
    VariableByteIntEncoder.write(bw, number_of_slices)
    
    slices.each do |slice_of_ints|
      FrameOfReferenceIntListEncoder.write(bw, slice_of_ints)
    end
  end
  
  def self.read(br)
    number_of_slices = VariableByteIntEncoder.read(br)
    
    ints = []
    number_of_slices.times do
      ints.concat(FrameOfReferenceIntListEncoder.read(br))
    end
    
    ints
  end
end

class SortedBlockEncoder
  def initialize(block_size = 128)
    @block_size = block_size
  end
  
  def encode(ints, sort_list = true)
    return "" if ints.empty?
    
    bw = BitWriter.new

    ints = delta_encode(sort_list ? ints.sort : ints)
    signed_start_int = ints.first
    remaining_ints = ints.drop(1)   # these are the int differences from the delta-encoded int list (i.e. all but the first int in the original list)
    
    number_of_ints, number_of_slices, signed_start_ints, orderings, block_sorted_ints = *SortedFrameOfReferenceIntListEncoder2.decompose_ints(remaining_ints)
    
    VariableByteIntEncoder.write_signed(bw, signed_start_int)
    VariableByteIntEncoder.write(bw, number_of_ints)
    VariableByteIntEncoder.write(bw, number_of_slices)
    BlockEncoder.write(bw, signed_start_ints, @block_size)
    FixedCountFrameOfReferenceIntListEncoder.write(bw, orderings, number_of_slices)
    BlockEncoder.write(bw, block_sorted_ints, @block_size)
    
    bw.close
    
    bw.to_s
  end

  def decode(bin_str)
    return [] if (bin_str.respond_to?(:empty?) && bin_str.empty?) || (bin_str.respond_to?(:eof?) && bin_str.eof?)
    
    br = BitReader.new(bin_str)
    
    signed_start_int = VariableByteIntEncoder.read_signed(br)
    number_of_ints = VariableByteIntEncoder.read(br)
    number_of_slices = VariableByteIntEncoder.read(br)
    
    signed_start_ints = BlockEncoder.read(br)
    orderings = FixedCountFrameOfReferenceIntListEncoder.read(br, number_of_slices)
    block_sorted_ints = BlockEncoder.read(br)
    
    delta_encoded_int_list = SortedFrameOfReferenceIntListEncoder2.compose_ints(number_of_ints, number_of_slices, signed_start_ints, orderings, block_sorted_ints)
    delta_encoded_int_list.unshift(signed_start_int)
    
    delta_decode(delta_encoded_int_list)
  end
end

class SortedFrameOfReferenceIntListEncoder2
  SLICE_SIZE = 5
  BITS_NEEDED_TO_REPRESENT_SORT_ORDER = 7   # 7 since 5!=120 < 2^7=128
  # SLICE_SIZE = 12
  # BITS_NEEDED_TO_REPRESENT_SORT_ORDER = 29   #29 since 12! < 2^29
  # SLICE_SIZE = 9
  # BITS_NEEDED_TO_REPRESENT_SORT_ORDER = 19
  
  # ints is an array of signed integers
  def self.write(bw, ints)
    if ints.count > 0
      number_of_ints, number_of_slices, signed_start_ints, orderings, block_sorted_ints = *decompose_ints(ints)
      
      VariableByteIntEncoder.write(bw, number_of_ints)
      VariableByteIntEncoder.write(bw, number_of_slices)
      FixedCountFrameOfReferenceIntListEncoder.write(bw, signed_start_ints, number_of_slices)
      FixedCountFrameOfReferenceIntListEncoder.write(bw, orderings, number_of_slices)
      FixedCountFrameOfReferenceIntListEncoder.write(bw, block_sorted_ints, number_of_slices * (SLICE_SIZE - 1))
    end
  end
  
  # assumes ints.length > 0
  def self.decompose_ints(ints)
    number_of_ints = ints.count
    slices = ints.each_slice(SLICE_SIZE).to_a
    number_of_slices = slices.count
    
    signed_start_ints = []
    orderings = []
    block_sorted_ints = []
    
    # process all but the last slice
    (0...(slices.length - 1)).each do |slice_index|
      slice_of_ints = slices[slice_index]
      sorted_slice_of_ints, ordering = *ArrayReordering.sort_and_identify_ordering(slice_of_ints)
      sort_order = identify_sort_order_inverse(ordering)

      delta_encoded_ints = delta_encode(sorted_slice_of_ints)
      signed_start_int = delta_encoded_ints.first
      remaining_ints = delta_encoded_ints.drop(1)   # these are the 4 int differences from the delta-encoded int list
  
      signed_start_ints << signed_start_int
      orderings << sort_order
      block_sorted_ints.concat(remaining_ints)
    end
    
    # process the last slice
    slice_of_ints = slices.last
    number_of_zeros = SLICE_SIZE - slice_of_ints.count
    slice_of_ints.concat(number_of_zeros.times.map{0})    # append a number of zeros to make sure the list is <SLICE_SIZE> elements in length
    sorted_slice_of_ints, ordering = *ArrayReordering.sort_and_identify_ordering(slice_of_ints)
    sort_order = identify_sort_order_inverse(ordering)

    delta_encoded_ints = delta_encode(sorted_slice_of_ints)
    signed_start_int = delta_encoded_ints.first
    remaining_ints = delta_encoded_ints.drop(1)   # these are the 4 int differences from the delta-encoded int list

    signed_start_ints << signed_start_int
    orderings << sort_order
    block_sorted_ints.concat(remaining_ints)
    
    [number_of_ints, number_of_slices, signed_start_ints, orderings, block_sorted_ints]
  end
  
  # ORDERING_TO_SORT_ORDER is of the form:
  # {[[0, 0], [1, 1], [2, 2], [3, 3], [4, 4]]=>1,
  #  [[0, 0], [1, 1], [2, 2], [4, 3], [3, 4]]=>2,
  #  [[0, 0], [1, 1], [3, 2], [2, 3], [4, 4]]=>3,
  #  [[0, 0], [1, 1], [4, 2], [2, 3], [3, 4]]=>4,
  #  [[0, 0], [1, 1], [3, 2], [4, 3], [2, 4]]=>5,
  #  ... }
  # ORDERING_TO_SORT_ORDER = [0,1,2,3,4].permutation.to_a.map{|triple| [0,1,2,3,4].zip(triple).sort_by{|pair| pair.last } }.zip(1..120).to_h
  ORDERING_TO_SORT_ORDER = (0...SLICE_SIZE).to_a.permutation.to_a.map{|triple| (0...SLICE_SIZE).to_a.zip(triple).sort_by{|pair| pair.last } }.zip(1..factorial(SLICE_SIZE)).to_h
  
  # SORT_ORDER_TO_ORDERING is of the form:
  # {1=>[[0, 0], [1, 1], [2, 2], [3, 3], [4, 4]],
  #  2=>[[0, 0], [1, 1], [2, 2], [4, 3], [3, 4]],
  #  3=>[[0, 0], [1, 1], [3, 2], [2, 3], [4, 4]],
  #  4=>[[0, 0], [1, 1], [4, 2], [2, 3], [3, 4]],
  #  5=>[[0, 0], [1, 1], [3, 2], [4, 3], [2, 4]],
  #  ... }
  SORT_ORDER_TO_ORDERING = ORDERING_TO_SORT_ORDER.invert
  
  def self.identify_sort_order(ordering)
    ORDERING_TO_SORT_ORDER[ordering.sort_by{|pair| pair.last }]
  end
  
  def self.identify_sort_order_inverse(ordering)
    ORDERING_TO_SORT_ORDER[ArrayReordering.inverse_ordering(ordering).sort_by{|pair| pair.last }]
  end

  # returns an array of signed integers
  def self.read(br)
    number_of_ints = VariableByteIntEncoder.read(br)
    number_of_slices = VariableByteIntEncoder.read(br)
    signed_start_ints = FixedCountFrameOfReferenceIntListEncoder.read(br, number_of_slices)
    orderings = FixedCountFrameOfReferenceIntListEncoder.read(br, number_of_slices)
    block_sorted_ints = FixedCountFrameOfReferenceIntListEncoder.read(br, number_of_slices * (SLICE_SIZE - 1))
    
    compose_ints(number_of_ints, number_of_slices, signed_start_ints, orderings, block_sorted_ints)
  end
  
  def self.compose_ints(number_of_ints, number_of_slices, signed_start_ints, orderings, block_sorted_ints)
    number_of_zeros = number_of_slices * SLICE_SIZE - number_of_ints
    
    ints = []
    
    # process all but the last slice
    (number_of_slices - 1).times do |i|
      signed_start_int = signed_start_ints[i]
      sort_order = orderings[i]
      ordering = identify_ordering(sort_order)
      
      delta_encoded_ints = block_sorted_ints.slice(i * (SLICE_SIZE - 1), SLICE_SIZE - 1)
      delta_encoded_ints.unshift(signed_start_int)
      
      sorted_ints = delta_decode(delta_encoded_ints)
      
      reordered_ints = ArrayReordering.reorder(sorted_ints, ordering)
      
      ints.concat(reordered_ints)
    end
    
    # process the last slice
    i = number_of_slices - 1
    signed_start_int = signed_start_ints[i]
    sort_order = orderings[i]
    ordering = identify_ordering(sort_order)
    
    delta_encoded_ints = block_sorted_ints.slice(i * (SLICE_SIZE - 1), SLICE_SIZE - 1)
    delta_encoded_ints.unshift(signed_start_int)
    
    sorted_ints = delta_decode(delta_encoded_ints)
    
    reordered_ints = ArrayReordering.reorder(sorted_ints, ordering)
    reordered_ints = reordered_ints.take(SLICE_SIZE - number_of_zeros)
    
    ints.concat(reordered_ints)
    # done processing the last slice
    
    ints
  end
  
  def self.identify_ordering(sort_order)
    SORT_ORDER_TO_ORDERING[sort_order]
  end
end

module ArrayReordering
  # ArrayReordering.reorder([3,2,1,0], [[0,3],[1,2],[2,1],[3,0]])
  # => [0,1,2,3]
  # ArrayReordering.reorder([10,7,2,1], [[0,3],[1,2],[2,1],[3,0]])
  # => [1,2,7,10]
  # ArrayReordering.reorder([1,2,7,10], [[3,0],[2,1],[1,2],[0,3]])
  # => [10,7,2,1]
  # ArrayReordering.reorder([1,2,7,10], [[0,2],[1,3],[2,0],[3,1]])
  # => [7,10,1,2]
  # ordering is an array of pairs, where each pair is of the form [source index, destination index]
  def self.reorder(array, ordering)
    ordering.sort_by{|pair| pair.last }.map do |pair|
      array[pair.first]
    end
  end
  
  IndexWrappedElement = Struct.new(:obj, :index)
  
  # returns an array of the form: [ sorted_array, ordering ]
  def self.sort_and_identify_ordering(unsorted_array)
    sorted_wrapped_array = unsorted_array.map.with_index{|obj, index| IndexWrappedElement.new(obj, index) }.sort_by{|wrapped_obj| wrapped_obj.obj }
    sorted_array = sorted_wrapped_array.map(&:obj)
    ordering = sorted_wrapped_array.map.with_index{|wrapped_obj, dest_index| [wrapped_obj.index, dest_index] }.sort_by{|pair| pair.last }
    [sorted_array, ordering]
  end
  
  # ArrayReordering.inverse_ordering([[0,3],[1,2],[2,1],[3,0]])
  # => [[3,0],[2,1],[1,2],[0,3]]
  def self.inverse_ordering(ordering)
    ordering.map{|pair| pair.reverse }
  end
end

class FixedCountFrameOfReferenceIntListEncoder
  # ints is an array containing exactly <count> signed integers
  def self.write(bw, ints, count)
    number_of_ints = ints.count
    raise "FixedCountFrameOfReferenceIntListEncoder.write given list of #{number_of_ints} integers but told to write #{count} integers." unless number_of_ints == count
    if number_of_ints > 0
      signed_min = ints.min
      offsets = ints.map {|int| int - signed_min }   # all offsets are guaranteed to be non-negative
      int_bit_size = bit_length(offsets.max)
    
      VariableByteIntEncoder.write_signed(bw, signed_min)
      VariableByteIntEncoder.write(bw, int_bit_size)
    
      offsets.each do |int|
        bw.write(int, int_bit_size)
      end
    end
  end

  # returns an array of <count> signed integers
  def self.read(br, count)
    signed_min_int = VariableByteIntEncoder.read_signed(br)
    int_bit_size = VariableByteIntEncoder.read(br)
    
    ints = []
    count.times do
      ints << (signed_min_int + br.read(int_bit_size))
    end
    
    ints
  end
end

class FrameOfReferenceIntListEncoder
  # ints is an array of signed integers
  def self.write(bw, ints)
    number_of_ints = ints.count
    if number_of_ints > 0
      signed_min = ints.min
      offsets = ints.map {|int| int - signed_min }   # all offsets are guaranteed to be non-negative
      int_bit_size = bit_length(offsets.max)
    
      VariableByteIntEncoder.write_signed(bw, signed_min)
      VariableByteIntEncoder.write(bw, number_of_ints)
      VariableByteIntEncoder.write(bw, int_bit_size)
    
      offsets.each do |int|
        bw.write(int, int_bit_size)
      end
    end
  end

  # returns an array of signed integers
  def self.read(br)
    signed_min_int = VariableByteIntEncoder.read_signed(br)
    number_of_ints = VariableByteIntEncoder.read(br)
    int_bit_size = VariableByteIntEncoder.read(br)
    
    ints = []
    number_of_ints.times do
      ints << (signed_min_int + br.read(int_bit_size))
    end
    
    ints
  end
end

class VariableByteIntEncoder
  # bw is a BitWriter object
  # int is an unsigned integer
  # returns the number of bytes written to the BitWriter
  def self.write(bw, int)
    bit_count = bit_length(int)
    bit_count_mod_7 = bit_count % 7
    evenly_divisible_bit_count = bit_count_mod_7 == 0 ? bit_count : bit_count + (7 - bit_count_mod_7)     # number of bits required to hold <int> in a whole number of 7-bit words
    seven_bit_word_count = evenly_divisible_bit_count / 7
    (1...seven_bit_word_count).each do |word_index|
      shift_amount = (seven_bit_word_count - word_index) * 7
      tmp_int = ((int >> shift_amount) & 0b01111111) | 0b10000000
      bw.write(tmp_int, 8)
    end
    tmp_int = int & 0b01111111
    bw.write(tmp_int, 8)
    
    seven_bit_word_count
  end
  
  # bw is a BitWriter object
  # int is an signed integer
  # returns the number of bytes written to the BitWriter
  def self.write_signed(bw, int)
    bit_count = bit_length(int) + 1   # + 1 to account for the sign bit we prefix the integer with
    bit_count_mod_7 = bit_count % 7
    evenly_divisible_bit_count = bit_count_mod_7 == 0 ? bit_count : bit_count + (7 - bit_count_mod_7)     # number of bits required to hold <int> in a whole number of 7-bit words
    seven_bit_word_count = evenly_divisible_bit_count / 7
    (1...seven_bit_word_count).each do |word_index|
      shift_amount = (seven_bit_word_count - word_index) * 7
      tmp_int = ((int >> shift_amount) & 0b01111111) | 0b10000000
      bw.write(tmp_int, 8)
    end
    tmp_int = int & 0b01111111
    bw.write(tmp_int, 8)
    
    seven_bit_word_count
  end
  
  # returns an unsigned int read from BitReader object <br>
  def self.read(br)
    int = br.read(8)
    sum = int & 0b01111111
    while Byte.msb(int) == 1
      int = br.read(8)
      sum = (sum << 7) | (int & 0b01111111)
    end
    sum
  end
  
  # returns an signed int read from BitReader object <br>
  def self.read_signed(br)
    int = br.read(8)
    count = 1
    sum = int & 0b01111111
    while Byte.msb(int) == 1
      int = br.read(8)
      count += 1
      sum = (sum << 7) | (int & 0b01111111)
    end
    u_to_s(sum, 7 * count - 1)
  end
end

class VariableLengthIntListEncoder
  def encode(ints)
    return "" if ints.empty?
    
    bw = BitWriter.new

    ints = delta_encode(ints.sort)
    signed_start_int = ints.first
    remaining_ints = ints.drop(1)   # these are the int differences from the delta-encoded int list (i.e. all but the first int in the original list)
    
    number_of_ints = remaining_ints.count
    signed_min = number_of_ints > 0 ? remaining_ints.min : 0
    remaining_ints = remaining_ints.map{|int| int - signed_min }   # after this line, all ints in remaining_ints are guaranteed to be non-negative
    
    VariableByteIntEncoder.write_signed(bw, signed_start_int)
    VariableByteIntEncoder.write_signed(bw, signed_min)
    VariableByteIntEncoder.write(bw, number_of_ints)
    
    remaining_ints.each do |int|
      VariableByteIntEncoder.write(bw, int)
    end
    
    bw.close
    
    bw.to_s
  end
  
  def decode(bin_str)
    return [] if (bin_str.respond_to?(:empty?) && bin_str.empty?) || (bin_str.respond_to?(:eof?) && bin_str.eof?)
    
    br = BitReader.new(bin_str)
    
    signed_start_int = VariableByteIntEncoder.read_signed(br)
    signed_min_int = VariableByteIntEncoder.read_signed(br)
    number_of_ints = VariableByteIntEncoder.read(br)
    
    delta_encoded_int_list = [signed_start_int]
    number_of_ints.times do
      delta_encoded_int_list << signed_min_int + VariableByteIntEncoder.read(br)
    end
    
    delta_decode(delta_encoded_int_list)
  end
end

class VariableLengthIntListEncoder2
  def encode(ints)
    return "" if ints.empty?
    
    bw = BitWriter.new

    ints = delta_encode(ints.sort)
    signed_start_int = ints.first
    remaining_ints = ints.drop(1)   # these are the int differences from the delta-encoded int list (i.e. all but the first int in the original list)
    
    number_of_ints = remaining_ints.count
    signed_min = number_of_ints > 0 ? remaining_ints.min : 0
    remaining_ints = remaining_ints.map{|int| int - signed_min }   # after this line, all ints in remaining_ints are guaranteed to be non-negative
    
    VariableByteIntEncoder.write_signed(bw, signed_start_int)
    VariableByteIntEncoder.write_signed(bw, signed_min)
    VariableByteIntEncoder.write(bw, number_of_ints)
    
    remaining_ints.each do |int|
      write_int(bw, int)
    end
    
    bw.close
    
    bw.to_s
  end
  
  def write_int(bw, int)
    bit_count = bit_length(int)
    bit_count_mod_3 = bit_count % 3
    evenly_divisible_bit_count = bit_count_mod_3 == 0 ? bit_count : bit_count + (3 - bit_count_mod_3)     # number of bits required to hold <int> in a whole number of 3-bit words
    seven_bit_word_count = evenly_divisible_bit_count / 3
    (1...seven_bit_word_count).each do |word_index|
      shift_amount = (seven_bit_word_count - word_index) * 3
      tmp_int = ((int >> shift_amount) & 0b0111) | 0b1000
      bw.write(tmp_int, 4)
    end
    tmp_int = int & 0b0111
    bw.write(tmp_int, 4)
  end
  
  def decode(bin_str)
    return [] if (bin_str.respond_to?(:empty?) && bin_str.empty?) || (bin_str.respond_to?(:eof?) && bin_str.eof?)
    
    br = BitReader.new(bin_str)
    
    signed_start_int = VariableByteIntEncoder.read_signed(br)
    signed_min_int = VariableByteIntEncoder.read_signed(br)
    number_of_ints = VariableByteIntEncoder.read(br)
    
    delta_encoded_int_list = [signed_start_int]
    number_of_ints.times do
      delta_encoded_int_list << signed_min_int + read_int(br)
    end
    
    delta_decode(delta_encoded_int_list)
  end
  
  def read_int(br)
    int = br.read(4)
    sum = int & 0b0111
    while Byte.msb(int, 3) == 1
      int = br.read(4)
      sum = (sum << 3) | (int & 0b0111)
    end
    sum
  end
end

class Byte
  # Byte.extract_int(0b11101010, 6, 2)
  # => 26
  # Byte.extract_int(0b11101010, 5, 2)
  # => 10
  def self.extract_int(fixnum_byte, msbit, lsbit)
    raise "most-significant-bit position cannot be less than the least-significant-bit position" if msbit < lsbit
    raise "least-significant-bit position must be >= 0 and most-significant-bit position must be < 8" if lsbit < 0 || msbit > 7
    (lsbit..msbit).reduce(0) {|sum, ith_lsbit| sum += fixnum_byte[ith_lsbit] * (2 ** (ith_lsbit - lsbit)) }
  end

  # Byte.extract_int_lr(0b11101010, 0, 2)
  # => 7
  # Byte.extract_int_lr(0b11101010, 1, 3)
  # => 6
  # Byte.extract_int_lr(0b11101010, 5, 7)
  # => 2
  def self.extract_int_lr(fixnum_byte, left_index, right_index)
    raise "left_index cannot be greater than the right_index" if left_index > right_index
    msbit = 7 - left_index
    lsbit = 7 - right_index
    extract_int(fixnum_byte, msbit, lsbit)
  end
  
  def self.msb(byte, most_significant_bit_position = 7)
    byte[most_significant_bit_position]
  end
end

# returns [unsigned, signed] bit packing directive
INT_PACKING = {
  8  => ["C", "c"],
  16 => ["S", "s"],
  32 => ["L", "l"],
  64 => ["Q", "q"]
}

def u_to_s(unsigned_int, most_significant_bit_index)
  msb = most_significant_bit(unsigned_int, most_significant_bit_index)
  if msb == 1
    -(1 << most_significant_bit_index) + flip_bit(unsigned_int, most_significant_bit_index)
  else
    unsigned_int
  end
end

def flip_bit(int, bit_index)
  int ^ (1 << bit_index)
end

def most_significant_bit(i, most_significant_bit_index)
  get_bit(i, most_significant_bit_index || most_significant_bit_position(i))
end

def most_significant_bit_position(i)
  len = bit_length(i)
  len == 0 ? 0 : len - 1
end

def get_bit(int, ith_least_significant_bit)
  (int >> ith_least_significant_bit) & 1
end

# <unsigned_int> should be non-negative
def bit_length(unsigned_int)
  # index_of_leftmost_one = int_to_binary_string(unsigned_int, 64).index("1") || 63    # if there are no ones, then we only need 1 bit to represent 0, so we pick 63 because 64 - 63 = 1

  # index_of_leftmost_one = int_to_binary_string(unsigned_int, 64).index("1") || 64
  # 64 - index_of_leftmost_one
  
  unsigned_int.bit_length
end

def total_bit_length(ints)
  ints.reduce(0) {|sum, int| sum + bit_length(int) }
end

# delta_encode [50, 60, 70, 75]
# => [50, 10, 10, 5]
def delta_encode(ints)
  new_ints = ints.take(1)
  i = 1
  len = ints.count
  while i < len
    new_ints << ints[i] - ints[i - 1]
    i += 1
  end
  new_ints
end

# delta_decode [50, 10, 10, 5]
# => [50, 60, 70, 75]
def delta_decode(delta_encoded_ints)
  list = []
  if !delta_encoded_ints.empty?
    delta_encoded_ints.reduce(0) {|sum, i| sum += i ; list << sum ; sum }
  end
  list
end

def int_to_binary_string(int, n)
  (n-1).downto(0).map{|i| int[i] }.join
end

def print_bits_i(int)
  31.downto(0) {|i| print int[i] }
end

def print_byte(int)
  7.downto(0) {|i| print int[i] }
end

def print_bytes(s)
  s.unpack("C*").each {|byte| print_byte(byte) ; print " " }
end



class RandomGaussian
  def initialize(mean, stddev, rand_helper = lambda { Kernel.rand })
    @rand_helper = rand_helper
    @mean = mean
    @stddev = stddev
    @valid = false
    @next = 0
  end

  def rand
    if @valid then
      @valid = false
      return @next
    else
      @valid = true
      x, y = self.class.gaussian(@mean, @stddev, @rand_helper)
      @next = y
      return x
    end
  end

  private
  def self.gaussian(mean, stddev, rand)
    theta = 2 * Math::PI * rand.call
    rho = Math.sqrt(-2 * Math.log(1 - rand.call))
    scale = stddev * rho
    x = mean + scale * Math.cos(theta)
    y = mean + scale * Math.sin(theta)
    return x, y
  end
end

def bits_per_int(bit_string, int_count)
  bit_string.length * 8 / int_count.to_f
end

def test_encoder(name, e, ints, average_uncompressed_bit_count_per_int)
  puts "-" * 80
  puts name

  encoded_ints = e.encode(ints)
  decoded_ints = e.decode(encoded_ints)

  bit_count_per_int = bits_per_int(encoded_ints, ints.count)
  puts "#{bit_count_per_int} bits per int"
  puts "#{bit_count_per_int / average_uncompressed_bit_count_per_int.to_f} compressed/uncompressed ratio"
  
  # print_bytes(encoded_ints)
  # puts
  # puts delta_encode(ints).join(",")
  
  sorted_ints = ints.sort
  sorted_decoded_ints = decoded_ints.sort
  
  # puts sorted_ints.join(",")
  # puts sorted_decoded_ints.join(",")
  puts (sorted_decoded_ints == sorted_ints ? "pass" : "test failed!")
end

def test_encoders(ints)
  puts "-" * 80
  total_bit_count = total_bit_length(ints)
  average_bit_count = total_bit_count / ints.count.to_f
  # puts "Total bit length"
  # puts total_bit_count
  puts "Average bit count per int"
  puts average_bit_count
  
  # e = GlobalFrameOfReferenceIntListEncoder.new
  # test_encoder(e.class.name, e, ints, average_bit_count)
  # 
  e = BinaryPackingIntListEncoder.new(5)
  test_encoder(e.class.name + " block=5", e, ints, average_bit_count)

  e = BinaryPackingIntListEncoder.new(128)
  test_encoder(e.class.name + " block=128", e, ints, average_bit_count)

  e = BinaryPackingIntListEncoder2.new(128)
  test_encoder(e.class.name + " block=128", e, ints, average_bit_count)

  e = SortedBlockEncoder.new(128)
  test_encoder(e.class.name + " block=128", e, ints, average_bit_count)

  # e = Bzip2BinaryPackingIntListEncoder.new(128)
  # test_encoder(e.class.name + " block=128", e, ints, average_bit_count)
  # 
  # e = DoubleEncodedBinaryPackingIntListEncoder.new(128)
  # test_encoder(e.class.name + " block=128", e, ints, average_bit_count)
  # 
  # e = VariableLengthIntListEncoder.new
  # test_encoder(e.class.name, e, ints, average_bit_count)
  # 
  # e = VariableLengthIntListEncoder2.new
  # test_encoder(e.class.name, e, ints, average_bit_count)
end

def main
  # puts "-" * 80
  # puts "BitReader"
  # 
  # br = BitReader.new(0b11011001.chr + 0b11110000.chr + 0b11000011.chr)
  # puts br.read(1) == 1
  # puts br.read(3) == 5
  # puts br.read(4) == 9
  # puts br.read(4) == 15
  # puts br.read(8) == 12
  # puts br.read(4) == 3
  # puts br.read(4) == nil
  # 
  # 
  # puts "-" * 80
  # puts "BitWriter"
  # 
  # bw = BitWriter.new
  # bw.write(0b110, 3)
  # bw.write(0b1100, 4)
  # bw.write(0b111111111, 9)
  # bw.write(0b1, 1)
  # bw.close
  # puts bw.to_s == 0b11011001.chr + 0b11111111.chr + 0b10000000.chr
  
  
  # ints = [7592454,8057713,5924959,9399853,6241879,4070139,7688230,3757258,7486242,8244638,6228178,161482,2470485,5962515,2624689,3799359,1546877,306202,6982131,4362269,8633545,753493,4095079,2796309,718196,6832753,7525345,4572470,1927389,8293458,7972469,4866196,5653824,8902475,2430364,9707710,5850061,8157747,3826904,4570551,6440485,6474097,8762983,1767138,239826,3610477,6571099,2836233,7386223,8991668,1094928,6688505,5249692,5584805,8725190,956508,317427,1255344,5817066,5345820,4342083,4435383,8520356,4201322,3571473,1426114,2369922,6687424,4444271,6570242,2507177,6734635,2335247,3385252,6323054,1373141,9786167,5774652,9930890,4509931,7566817,2769885,5360844,7423987,6387832,5971114,4469765,8139452,9711273,3440395,5427475,9633641,510364,7804969,4061926,2242454,8684594,782023,4091359,5108495,2217767,3381546,902742,8674758,3402657,8557477,3957893,1909256,5488320,3155570,8938676,865499,4403375,8102827,7002791,8568699,5573582,5506297,8387228,6056975,53604,8333271,3663781,2977303,1722624,2897331,3373614,3700620,5813476,7948432,8723936,5677870,579721,4644291,580807,6119296,4503353,5206245,8360023,1260526,3307395,6740003,2781798,517635,4125092,9213145,6224730,1887491,8353133,8518895,5702411,894704,7468554,6408570,4580210,5260254,4999899,729700,275878,9009094,8487343,9741509,3452020,3890865,8274281,703884,418727,7338714,5152918,5377466,9733574,7536829,9397866,1674076,8816610,987821,3952043,5911678,967561,353827,8521785,4694183,198464,2891472,4061964,7133375,966826,8340598,8429118,8356535,9884830,6137864,5487561,8292301,6990981,1423196,7527965,2352525,3326121,5979849,7894549,4089493,6425025,8197381,14342,9589717,1344892,7667787,1889487,4078816,1023255,6428598,9965509,5459705,8717269,3187470,9667184,8777424,6085042,9038461,701033,8537662,2159149,9741919,9879560,7896513,2906817,12698,5381462,8146289,7894784,5131904,2548131,399641,1997611,4773996,6005717,8626499,271473,2475238,3100563,9732257,1173181,1908272,2225759,4774228,9352628,5342404,9376533,3356535,7683904,4244802,2171267,8829091,3125337,7268802,8927467,6751242,1453362,2873965,8593623,2263438,128765,3553057,8182192,9952807,7882816,215332,186309,8734966,1281951,6322488,378605,5306119,6885271,5564419,3281521,9874101,9060315,9144157,2281079,921222,513726,4531130,8617102,8622072,6417299,6937340,6541570,4523119,8147152,2019678,5105743,3581564,4544267,7047028,9211226,7514348,1604334,4479224,5280344,2487996,8056038,8564727,5545501,1077890,9449962,1251986,605243,7846533,6290466,3371158,4134876,9512375,5511008,2205675,579762,6974262,2405032,9093048,7347768,5037241,501795,6087048,4280242,1381831,4957577,6454370,9676642,3116238,7679446,7905845,9488337,5155830,7159129,2390048,4872727,7966832,5460871,7461755,5687494,9932886,7977082,5404002,8907924,9902373,6391288,4491828,9563974,5429598,5171640,3420771,3928464,4688137,1650105,3176414,4757740,4276374,7822885,6879999,5794074,47922,8419415,3312051,8686224,4791652,1220620,3389424,6618772,9260424,9094459,5527243,7753304,1855287,4533530,2228776,6939244,1618087,3801868,9362572,9321385,5770179,6360243,9307114,980004,3642071,3046759,7269749,2492956,2067750,7192210,6523634,9386694,2499454,215785,7096736,2072859,1630782,5161546,6867800,4140529,2316859,8052644,5178580,6059138,9238551,5637686,9089493,720832,2427984,3614607,4960852,2891918,1379055,1689620,3097102,2043373,6590482,1148457,7948043,79371,2577065,8012590,2542187,3762732,1412310,6877319,9728875,2322636,5971830,625575,855921,2369666,1155223,7612996,9583557,3204403,6110405,8404240,2688581,5687952,8001673,4975782,9413238,9837566,2469146,227282,7994920,4069401,9853368,3141417,897781,7613323,9183450,9473223,6204223,1488104,8680573,1104634,3456671,1167274,8995740,314696,7890485,2306076,5638973,3864184,7286743,1713495,4658584,174573,1123501,2831327,8929129,4599355,8890453,3402788,323613,5503826,8252150,3251396,7150846,7498670,3932242,8959403,4194538,6922191,1331208,2385808,9195184,2636624,6661598,6541727,5506645,2726430,7483735,3852720,7361156,9617707,220671,6385549,4900425,3370500,5325982,6763793,6558658,9162511,7551468,9890394,8643262,9209388,1096721,1636128,1885043,6991958,498206,307711,1846078,2082486,1115444,3035756,1183017,860981,8269120,961426,8936365,5526211,4297485,7112669,4227600,3311053,6733034,1475743,772830,7548586,2131350,7489827,4210781,7758238,4392951,602785,3752329,4825515,6058938,6093932,8446428,8765037,8886468,3430612,6494778,5340198,6031121,7857224,3905491,2160588,5703888,6781512,2360525,8544929,5891339,8286639,1662678,2080842,9609765,6184432,9955180,2976384,6602179,4538634,2234772,5748880,9742735,8148032,2720917,1277361,4700533,1772862,4399662,2934532,818840,4134729,6200651,179526,8077281,4890252,6477067,6101810,8415757,3982268,5567525,3292012,9973912,5024870,9396574,1940545,5205508,8564956,4835709,9486292,5527096,7419262,584128,5378471,7919913,5310228,3450790,2654857,714714,8771183,1871471,2831065,9382186,2597295,5131153,6364658,1474821,8440864,9774369,4164211,7109407,8294439,3964773,5386192,4318026,8421944,7502749,7269000,7456862,1377785,1144074,9574640,2600874,1839926,8219822,6461740,5354931,7397529,3589294,2892988,8060062,8118640,4490535,8606753,5790624,2933885,3224316,7702111,4764475,1961530,3519771,2304407,3862594,2245574,3038987,5133122,5869380,515854,9039030,3025976,2306495,698440,3657679,6226970,2975440,1940347,2772092,9414982,782511,3721962,2642685,896260,1486081,2816325,5811294,240168,2344319,9123789,6535035,3747184,6313055,1621036,7493984,7383537,9062311,2531772,3038023,325715,3654843,5963346,7523875,9779132,9335386,3563454,4152,7620889,1826333,4416289,6850890,2916185,7388214,760303,6925142,3706945,3354191,7741259,6992599,2277230,3357264,9453530,3717478,7913434,4426377,50077,181390,9636268,8425915,6096704,9571443,2588481,8066343,3719949,9563242,3821275,4589969,9958753,164440,533232,3531914,212272,1843221,9516869,1090413,2119608,683115,7824944,3191863,820756,8322887,3711897,2378170,261163,2272910,780378,3635124,3679283,4913520,6881587,4393643,964673,5649698,4799108,8533625,373006,3398948,4567155,9169983,3767937,8334590,4517743,5263084,7976611,6966081,5504281,70427,1365753,7632834,3950066,2724637,825750,2890904,5368314,9747363,2386900,512647,2217374,3637240,9633843,3109061,9962848,6054912,6258018,4905840,6450575,8332447,8223616,872499,2873497,3256216,8602229,5791571,3352506,4811507,3606152,2610617,9223370,8441114,2088572,4415167,1547596,6392806,537735,692672,4875461,3319085,751398,2385215,9805361,1289240,9111681,8449278,3798079,4434527,4307734,3617515,2302781,3763512,7975793,6047910,7219557,5676871,185498,9238405,5897215,2638929,1987561,6562478,4515401,9780227,9366415,8261068,424669,3893892,3730823,4986989,1183835,4077678,2670282,9841440,7174521,166230,8640457,1587930,2272315,2590594,1751470,8009272,6795301,1605212,4689699,7786682,7753854,7705228,3591692,2190496,7329496,8891846,6449256,9951573,8519359,6068619,8287572,7080684,9132735,3121817,6289853,2721681,1755277,4617095,1492442,9762325,9093817,990147,9688837,4982775,7735520,9379943,7949313,9301585,4880270,7708591,711467,8724262,5657682,8093130,4729021,6896621,9723177,4460957,6531283,3600113,1026649,2763646,7166844,2517985,9260701,7679839,6444755,4284505,2998373,3140342,6467261,3887164,1292741,187360,4462672,8257868,2835872,9477973,7246679,2756365,4470533,1349735,3789978,7341861,1939862,1086293,3006708,7634271,2243412,3852566,8253924,9439832,6676430,2825751,7790749,4863111,2895707,1334781,2325432,4450621,1191941,4613182,1232861,8790244,580530,4764535,6536971,8216458,1528391,320978,239127,426187,7549204,8867356,8672097,1250208,4133301,1104019,6343980,2015913,6903593,8745065,970564,4397096,6036061,3638262,8889210,1371219,3378510,2857279,5096990,5994801,3998220,4081450,5530728,3440641,235629,7666507,9553878,1266348,6532208,3355162,4831748,1406922,5790344,4393645,2331622,3504298,581854,702039,2999459,1410261,9368031,266647,8703072,1505070,4928763,7067911,447296,2659207,7812446,8905758,2600626,4777574,7586127,3201512,3251980,9179223,7793467,4027264]
  # expected_encoded_ints_bytes = [160,56,8,41,129,0,16,33,57,6,67,131,3,8,66,13,158,65,142,34,199,192,201,127,164,11,101,6,213,32,110,19,48,7,31,15,227,3,2,3,242,43,55,53,199,11,203,1,156,18,237,25,170,32,114,13,129,2,146,1,45,81,218,21,67,18,177,17,12,118,75,5,188,27,32,10,130,13,182,10,34,8,13,109,167,74,194,21,182,82,3,74,101,23,13,5,197,82,76,198,181,13,220,33,80,8,194,4,14,8,39,6,204,60,196,17,110,163,217,0,0,2,215,0,236,3,238,8,185,72,184,9,113,79,67,224,155,37,44,22,95,9,248,3,197,7,12,29,118,12,134,13,113,10,35,34,123,84,153,8,6,26,113,48,198,29,83,6,68,1,191,141,192,7,83,19,89,117,178,19,155,17,125,27,47,86,148,5,235,5,200,19,56,72,7,137,173,19,13,12,134,8,64,2,182,11,146,36,183,30,96,8,237,129,43,13,25,200,0,32,170,15,239,17,122,6,216,28,89,2,62,42,17,31,80,80,52,16,246,26,69,46,234,22,234,38,67,3,9,31,129,111,222,47,168,67,154,6,201,12,245,20,21,129,70,129,0,16,21,248,42,63,17,40,27,179,12,231,149,125,13,47,38,185,18,37,61,204,20,148,6,188,17,94,4,48,10,18,97,61,12,69,7,59,41,192,10,160,105,170,83,13,2,212,39,156,7,33,16,44,48,142,90,83,71,112,2,9,156,200,63,78,2,168,49,133,10,191,37,76,20,28,53,211,48,87,43,192,59,242,92,125,34,227,111,232,14,25,45,143,21,150,208,25,52,83,12,25,10,99,35,51,62,114,52,62,8,202,7,6,72,155,3,18,70,15,47,243,1,31,0,0,81,51,100,233,38,124,70,184,13,239,91,201,94,115,19,47,30,105,5,166,23,0,120,118,45,24,107,209,4,217,40,241,74,87,58,133,44,237,0,195,30,114,11,3,22,166,29,60,2,248,7,172,69,2,33,231,1,141,16,26,14,67,84,0,5,148,5,191,0,221,39,182,21,203,10,38,23,104,13,99,34,170,31,72,30,122,34,239,0,58,31,114,26,191,1,139,3,126,11,134,57,194,88,226,8,134,150,184,4,117,17,203,49,16,18,154,24,156,29,101,41,114,53,21,39,233,22,114,112,64,43,210,129,3,129,0,16,7,190,25,170,12,128,0,117,37,140,54,117,46,28,8,126,14,41,47,9,16,123,42,192,70,248,125,205,2,121,11,9,6,126,116,108,27,238,23,220,8,28,37,103,56,44,77,173,36,79,20,63,0,131,17,62,0,230,81,179,62,215,1,81,65,168,1,181,1,59,3,171,10,28,5,213,36,139,36,21,68,161,2,4,159,73,3,45,3,20,81,203,3,187,27,206,74,193,37,177,8,88,3,65,29,217,196,36,13,2,32,175,27,134,21,72,13,61,58,26,3,176,54,198,80,233,42,173,16,166,37,46,10,200,77,70,105,69,1,197,16,9,98,86,40,120,59,148,13,199,3,99,26,247,26,249,102,142,6,18,3,72,4,218,2,86,51,49,2,15,9,21,18,157,11,89,13,247,15,201,36,177,13,250,0,0,69,188,37,238,37,180,0,115,39,34,4,75,17,168,185,136,59,238,46,236,82,20,40,26,30,208,38,232,29,175,8,219,32,98,23,20,16,98,15,159,10,217,68,70,7,193,3,123,14,94,49,97,10,145,23,83,60,11,82,214,24,50,18,213,21,74,9,36,7,90,2,129,0,16,34,155,63,231,20,23,19,63,21,96,3,10,17,71,86,23,31,163,4,254,9,203,75,205,21,251,100,60,0,152,38,144,6,52,89,194,14,115,11,209,45,77,89,187,14,192,69,158,7,183,22,216,26,222,68,85,62,78,113,114,135,100,0,36,29,11,2,224,29,113,4,112,10,72,31,105,7,72,14,134,117,59,32,15,5,146,0,145,22,19,92,128,118,117,26,126,36,241,65,177,67,48,123,82,15,26,16,165,50,178,40,7,40,50,93,247,78,216,119,216,2,178,0,0,13,121,10,4,14,127,46,14,4,96,39,102,31,212,3,86,34,182,24,204,40,94,6,177,27,179,2,254,33,241,44,45,5,11,45,3,25,176,21,92,9,36,20,254,31,73,9,94,19,238,21,255,89,102,13,66,7,125,30,58,38,29,36,168,54,1,15,71,106,58,55,211,115,111,6,24,17,130,24,204,111,70,112,45,26,77,0,58,36,243,0,230,13,16,54,252,29,30,48,109,54,182,24,87,15,119,107,8,12,11,25,129,10,172,18,199,38,252,39,187,21,37,29,254,59,137,112,140,12,201,58,80,129,19,129,0,16,26,190,15,227,49,219,96,248,47,192,232,210,33,158,10,45,87,239,2,92,4,47,76,193,10,205,21,193,38,219,26,137,104,157,2,78,169,36,40,175,10,123,66,217,100,28,15,122,60,247,54,245,8,11,12,197,35,4,22,134,28,155,35,45,3,90,11,28,17,231,68,255,91,30,7,184,117,8,3,251,103,175,2,100,59,255,1,52,7,77,0,201,16,120,58,208,2,226,0,0,13,10,57,34,73,83,11,143,23,22,43,68,205,254,4,116,41,82,15,139,14,127,74,98,3,84,37,5,1,55,55,232,5,50,175,45,82,160,16,230,60,185,0,133,3,32,9,52,66,177,7,243,13,115,128,80,74,228,85,52,22,97,55,236,51,78,146,33,2,172,29,197,2,57,30,192,57,213,42,17,98,169,18,185,45,182,26,199,7,124,7,24,0,53,36,118,63,148,7,67,26,81,10,65,19,95,33,0,34,40,71,245,181,85,62,200,13,97,79,136,8,45,4,37,52,242,62,120,123,200,1,210,87,170,36,70,1,163,81,43,62,244,16,172,81,8,8,88,12,237,5,91,61,1,33,134,129,29,129,0,16,29,145,13,88,45,210,16,17,16,248,4,138,14,54,28,45,20,244,26,23,10,253,68,146,112,27,29,68,3,0,10,110,6,243,17,90,0,0,65,134,14,79,29,183,2,188,75,26,45,20,64,52,166,173,57,83,42,85,3,156,173,84,5,164,20,91,43,74,48,106,68,154,53,64,145,175,70,60,65,113,36,146,9,219,5,151,13,199,43,185,26,159,72,9,10,234,47,9,6,211,104,56,31,88,30,32,33,245,3,52,1,228,39,51,172,48,80,246,49,72,62,23,48,226,12,33,80,69,67,162,31,190,29,134,29,96,68,124,106,54,105,85,85,206,0,41,2,80,65,197,166,100,35,101,11,174,22,118,51,175,86,208,9,225,7,42,35,198,84,72,17,216,127,206,18,128,25,242,58,176,9,46,13,100,15,160,17,177,15,82,44,178,36,154,5,33,9,159,34,3,45,80,1,205,8,59,59,88,74,209,24,26,79,161,0,170,28,241,46,12,5,0,125,79,4,99,44,238,0,236,15,68,16,73,53,156,11,144,12,134,104,148,21,206,46,112,1,137,16,131,110,127,15,70,10,1,129,101,129,0,16,44,9,28,80,39,226,7,38,83,112,40,222,99,19,29,16,14,251,0,6,5,220,35,143,28,192,24,106,108,253,0,160,2,140,67,138,21,32,12,23,2,77,0,242,68,201,25,124,28,202,12,17,155,145,12,93,5,166,8,72,23,164,41,213,61,4,36,252,60,224,80,103,25,208,2,122,2,139,37,14,94,152,58,112,73,160,12,63,13,237,81,57,28,115,6,9,14,131,11,155,30,143,19,68,47,97,2,192,17,148,3,160,2,240,110,59,36,115,2,83,4,66,22,147,48,18,12,101,12,187,105,96,65,143,44,24,13,101,8,252,14,158,11,158,44,253,0,21,19,221,10,61,147,204,122,91,0,235,3,0,4,176,45,91,14,224,27,126,48,31,27,109,0,0,13,186,96,119,32,185,16,199,39,136,18,133,16,102,26,161,26,27,10,16,111,190,9,128,21,210,14,208,5,121,64,235,54,144,25,38,0,97,2,187,37,75,38,142,69,25,7,33,23,29,23,124,49,47,102,25,47,220,148,148,73,195,9,209,3,246,4,140,40,160,11,238,7,145,75,114,5,153,27,95,8,34,129,18,103,16,80,101,125,119,15,86,51,152,114,37,1,167,82,147,7,58,105,156,13,81,2,111,1,240,66,180,46,186,34,96,44,12,71,32,28,158,35,134,15,241,45,68,54,234,6,156,6,237,39,95,58,41,0,0,84,223,0,131,159,34,21,7,55,45,54,31,66,200,38,70,14,113,5,190,32,164,12,192,8,49,17,10,38,6,4,122,7,49,51,183,6,62,96,128,39,0,13,94,76,91,17,252,31,237,7,107,93,84,16,252,143,255,36,2,2,74,28,155,11,235,34,67,23,126,77,190,30,116,61,172,0,56,8,231,120,50,36,96,47,17,73,39,13,89,45,238,21,176,12,164,4,147,30,109,1,8,2,158,17,130,57,224,46,122,18,9,3,181,22,162,74,104,125,59,14,144,46,6,80,107,20,193,20,4,21,42,46,57,110,211,7,58,72,109,4,64,8,179,13,99,15,109,9,211,32,65]
  # # ints = [1,2,50]
  # # expected_encoded_ints_bytes = [1,1,1,2,6,2,240]
  # total_bit_count = total_bit_length(ints)
  # average_bit_count = total_bit_count / ints.count.to_f
  # sorted_ints = ints.sort
  # e = BinaryPackingIntListEncoder.new(128)
  # encoded_ints = e.encode(ints)
  # puts "encoded_ints:"
  # puts encoded_ints.each_byte.to_a.join(',')
  # bit_count_per_int = bits_per_int(encoded_ints, ints.count)
  # puts "#{bit_count_per_int} bits per int"
  # puts "#{bit_count_per_int / average_bit_count.to_f} compressed/uncompressed ratio"
  # decoded_ints = e.decode(encoded_ints)
  # print_bytes(encoded_ints)
  # puts expected_encoded_ints_bytes == encoded_ints.each_byte.to_a ? "encoding pass" : "encoding fail"
  # puts sorted_ints == decoded_ints ? "decoding pass" : "decoding fail"


  upper_bound = 2 ** 29
  
  max_bits_to_represent_lehmer_code = 128*7
  
  ints = (2 ** 15).times.map { rand(upper_bound) }
  min, max = ints.minmax
  total_bit_count = total_bit_length(ints)
  average_bit_count = total_bit_count / ints.count.to_f
  puts "#{ints.count} ints between #{min} and #{max}"
  puts "#{total_bit_count} total bits"
  puts "#{average_bit_count} bits per int"
  puts

  deints = delta_encode(ints.sort)
  min, max = deints.minmax
  total_bit_count = total_bit_length(deints)
  average_bit_count = total_bit_count / deints.count.to_f
  puts "#{deints.count} ints between #{min} and #{max}"
  puts "#{total_bit_count} total bits"
  puts "#{average_bit_count} bits per int"
  puts

  dedeints = delta_encode(deints.sort)
  min, max = dedeints.minmax
  total_bit_count = total_bit_length(dedeints)
  average_bit_count = total_bit_count / dedeints.count.to_f
  puts "#{dedeints.count} ints between #{min} and #{max}"
  puts "#{total_bit_count} total bits"
  puts "#{average_bit_count} bits per int"
  puts
  
  deint_slices = deints.each_slice(128)
  min, max = deints.minmax
  deslices = deint_slices.map {|slice| delta_encode(slice.sort) }
  total_bit_count = deslices.map {|deslice| total_bit_length(deslice) }.reduce(:+)
  average_bit_count = total_bit_count / deints.count.to_f
  puts "#{deints.count} ints between #{min} and #{max}"
  puts "#{total_bit_count} total bits"
  puts "#{average_bit_count} bits per int"
  puts

  total_with_lehmer_codes = total_bit_count + max_bits_to_represent_lehmer_code * deint_slices.count
  average_with_lehmer_codes = total_with_lehmer_codes / ints.count.to_f
  puts "#{total_with_lehmer_codes} total bits"
  puts "#{average_with_lehmer_codes} bits per int"
  
  return
  
  
  puts
  puts "=" * 80
  puts "[1,2,50]"
  
  ints = [1,2,50]
  test_encoders(ints)
  
  
  puts
  puts "=" * 80
  puts "[1,2,3,4,5,50,60,70,80,90,100]"
  
  ints = [1,2,3,4,5,50,60,70,80,90,100]
  test_encoders(ints)
  
  
  puts
  puts "=" * 80
  puts "2250 normally distributed floats (mu = 10000, sigma = 5000) rounded to nearest thousandth"
  
  r = RandomGaussian.new(10000, 5000)
  ints = 2250.times.map{ (r.rand * 1000).round.to_i }
  test_encoders(ints)
  
  
  puts
  puts "=" * 80
  puts "10000 normally distributed floats (mu = 10000, sigma = 5000) rounded to nearest thousandth"
  
  r = RandomGaussian.new(10000, 5000)
  ints = 10000.times.map{ (r.rand * 1000).round.to_i }
  test_encoders(ints)
  
  
  puts
  puts "=" * 80
  puts "100000 normally distributed floats (mu = 10000, sigma = 5000) rounded to nearest thousandth"
  
  r = RandomGaussian.new(10000, 5000)
  ints = 100000.times.map{ (r.rand * 1000).round.to_i }
  test_encoders(ints)
  
  
  puts
  puts "=" * 80
  puts "10000 uniformly distributed ints in the range [0, 100)"
  
  ints = 10000.times.map { rand(100) }
  test_encoders(ints)
  
  
  puts
  puts "=" * 80
  puts "10000 uniformly distributed ints in the range [0, 10000000)"
  
  ints = 10000.times.map { rand(10000000) }
  test_encoders(ints)
  
  
  puts
  puts "=" * 80
  puts "100000 uniformly distributed ints in the range [0, 10000000)"
  
  ints = 100000.times.map { rand(10000000) }
  test_encoders(ints)

  upper_bound = 2 ** 29

  puts
  puts "=" * 80
  puts "2^15 uniformly distributed ints in the range [0, #{upper_bound})"
  
  ints = (2 ** 15).times.map { rand(upper_bound) }
  test_encoders(ints)
  
  
  puts
  puts "=" * 80
  puts "2^15 normally distributed floats (mu = #{upper_bound / 2}, sigma = #{upper_bound / 16})"
  
  r = RandomGaussian.new(upper_bound / 2, upper_bound / 16)
  ints = (2 ** 15).times.map{ (r.rand).round.to_i }
  test_encoders(ints)


  puts
  puts "=" * 80
  puts "2^25 uniformly distributed ints in the range [0, #{upper_bound})"
  
  ints = (2 ** 25).times.map { rand(upper_bound) }
  test_encoders(ints)
  
  
  puts
  puts "=" * 80
  puts "2^25 normally distributed floats (mu = #{upper_bound / 2}, sigma = #{upper_bound / 16})"
  
  r = RandomGaussian.new(upper_bound / 2, upper_bound / 16)
  ints = (2 ** 25).times.map{ (r.rand).round.to_i }
  test_encoders(ints)
end

main if $0 == __FILE__