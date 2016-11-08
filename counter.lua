--babbler--
--use Russ code as pseudo code--
-- generate text--

function createTable(tokens, n)
  local table_length = #tokens + 1
  local lookup_table = {}
  local shing_table = {}
  for i = 0, #tokens do
    local shingle = {}
    for j = 1, n do
      shingle[j] = tokens[(i + j -1 ) % table_length ]
    end
	--build up the lookup table with shingle
    local suffix = tokens[(i + n - 1) % table_length]
		local key_minus = {}
			--remove the last value
		for k=1,#shingle - 1 do
			key_minus[k] = shingle[k]
		end
		--turn list into a string like ' '.join()
    		local key = table.concat(key_minus, " ")
		lookup_table[i] = key
    -- base case
    if lookup_table[key] == nil then
      lookup_table[key] = {}
    end
    -- append to the array with elt + 1
    lookup_table[key][#lookup_table[key] + 1] = suffix
    -- add last case
    shingle[n] = suffix
    shing_table[i+1] = shingle
  end
  return lookup_table, shing_table
end

function babble(start, table, size)
	local ngram = {}
	print('\n')
	io.write("$$$$",' ')
	ngram = start
	--write user defined amount of words
	for i=0,size do
		newgram = {}
		--remove the first element from each ngram
		for j=1,#ngram - 1 do
			newgram[j] = ngram[j+1]
		end
	--turn list into a string
	local prefix = ""
	for index = 1, #newgram do
		--handle base case without adding a space
		if index == 1 then 
			prefix = prefix..newgram[index] 
		else
			prefix = prefix.." "..newgram[index]
		end
	end
	--find matching values to keys
	candidates = table[prefix]
	suffix = candidates[math.random(#candidates)]
	newgram[#newgram + 1] = suffix
	--make a new ngram for next iteration
	ngram = newgram
	io.write(suffix, " ")
	end
	print("$$$$",'\n')
end

--bring in filename, amount of words, and size of ngrams
function main(file, size, n)
	math.randomseed( os.time() )
	local tokens = {}
	local offset = 0;
	local raw_text = creadfile(file)
	local token, offset = ccreateTokens(raw_text, 0)
	local index = 0
	--build a token table--
  	while offset < string.len(raw_text) do
    		tokens[index] = token
    		index = index + 1
    		token, offset = ccreateTokens(raw_text, offset)
 	end
	tokens[index] = token
	--call functions to create lookup table and output babbled text
 	lookup_table, shing_table = createTable(tokens, n)
	babble(shing_table[math.random(index)],lookup_table, size)
	
end
