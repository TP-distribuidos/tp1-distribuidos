#Usage ./check_ouputs.sh (use -q5 flag for checking q5)

# Colors for nice output formatting
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Path to output directory (relative)
OUTPUT_DIR="client/output"

# Default to not checking Q5
CHECK_Q5=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -q5)
            CHECK_Q5=true
            shift
            ;;
        *)
            # Unknown option
            echo "Unknown option: $1"
            echo "Usage: $0 [-q5]"
            echo "  -q5  Also check Q5 output files"
            exit 1
            ;;
    esac
done

# Function to check Q1 output (25 movies with genres)
check_q1() {
    local file=$1
    local status=0
    local expected_count=25
    local found_count=0
    
    printf "${BLUE}→ Checking Q1 (Movies and Genres)...${NC}\n"
    
    # Array of expected entries
    declare -a expected=(
        '{"Movie": "La Ci\u00e9naga", "Genres": ["Comedy", "Drama"]}'
        '{"Movie": "Plata quemada", "Genres": ["Crime"]}'
        '{"Movie": "Nicotina", "Genres": ["Drama", "Action", "Comedy", "Thriller"]}'
        '{"Movie": "El abrazo partido", "Genres": ["Drama", "Foreign"]}'
        '{"Movie": "Lugares comunes", "Genres": ["Drama"]}'
        '{"Movie": "Whisky", "Genres": ["Comedy", "Drama", "Foreign"]}'
        '{"Movie": "Aparecidos", "Genres": ["Horror", "Thriller", "Mystery"]}'
        '{"Movie": "El ni\u00f1o pez", "Genres": ["Drama", "Thriller", "Romance", "Foreign"]}'
        '{"Movie": "Cleopatra", "Genres": ["Drama", "Comedy", "Foreign"]}'
        '{"Movie": "Roma", "Genres": ["Drama", "Foreign"]}'
        '{"Movie": "La ni\u00f1a santa", "Genres": ["Drama", "Foreign"]}'
        '{"Movie": "El Aura", "Genres": ["Crime", "Drama", "Thriller"]}'
        '{"Movie": "Bomb\u00f3n, el perro", "Genres": ["Drama"]}'
        '{"Movie": "Familia rodante", "Genres": ["Drama", "Comedy"]}'
        '{"Movie": "El m\u00e9todo", "Genres": ["Drama", "Thriller"]}'
        '{"Movie": "En la ciudad sin l\u00edmites", "Genres": ["Thriller", "Drama"]}'
        '{"Movie": "Todas las azafatas van al cielo", "Genres": ["Drama", "Romance", "Foreign"]}'
        '{"Movie": "Tetro", "Genres": ["Drama", "Mystery"]}'
        '{"Movie": "El secreto de sus ojos", "Genres": ["Crime", "Drama", "Mystery", "Romance"]}'
        '{"Movie": "Liverpool", "Genres": ["Drama"]}'
        '{"Movie": "La mujer sin cabeza", "Genres": ["Drama", "Mystery", "Thriller"]}'
        '{"Movie": "El \u00faltimo verano de La Boyita", "Genres": ["Drama"]}'
        '{"Movie": "Conversaciones con mam\u00e1", "Genres": ["Comedy", "Drama", "Foreign"]}'
        '{"Movie": "La educaci\u00f3n de las hadas", "Genres": ["Drama"]}'
        '{"Movie": "La buena vida", "Genres": ["Drama"]}'
    )
    
    # Check file exists
    if [ ! -f "$file" ]; then
        printf "  ${RED}✗ File $file not found${NC}\n"
        return 1
    fi
    
    # Check line count - fail if not exact match
    local actual_count=$(grep -c . "$file")
    if [ "$actual_count" -ne $expected_count ]; then
        printf "  ${RED}✗ File has $actual_count records, expected exactly $expected_count${NC}\n"
        status=1
    fi
    
    # Check each expected line
    for item in "${expected[@]}"; do
        # Escape special characters for grep
        if grep -q "$(echo "$item" | sed 's/[]\/$*.^[]/\\&/g')" "$file"; then
            ((found_count++))
        else
            printf "  ${RED}✗ Missing:${NC} $item\n"
            status=1
        fi
    done
    
    # Summary for this file
    if [ $status -eq 0 ]; then
        printf "  ${GREEN}✓ All $expected_count records found!${NC}\n"
    else
        printf "  ${RED}✗ Found $found_count of $expected_count expected records${NC}\n"
    fi
    
    return $status
}

# Function to check Q3 output (max/min ratings)
check_q3() {
    local file=$1
    local expected='{"max": {"id": "80717", "avg": 5.0, "name": "Violeta se fue a los cielos"}, "min": {"id": "69278", "avg": 2.75, "name": "Fase 7"}}'
    
    printf "${BLUE}→ Checking Q3 (Max/Min Ratings)...${NC}\n"
    
    # Check file exists
    if [ ! -f "$file" ]; then
        printf "  ${RED}✗ File $file not found${NC}\n"
        return 1
    fi
    
    # Check line count - expected to have exactly 1 record
    local actual_count=$(grep -c . "$file")
    if [ "$actual_count" -ne 1 ]; then
        printf "  ${RED}✗ File has $actual_count records, expected exactly 1${NC}\n"
        return 1
    fi
    
    # Check content
    if grep -q "$(echo "$expected" | sed 's/[]\/$*.^[]/\\&/g')" "$file"; then
        printf "  ${GREEN}✓ Found expected max/min ratings record${NC}\n"
        return 0
    else
        printf "  ${RED}✗ Expected record not found${NC}\n"
        printf "  ${YELLOW}  Expected:${NC} $expected\n"
        return 1
    fi
}

# Function to check Q4 output (actor movie count)
check_q4() {
    local file=$1
    local status=0
    local expected_count=10
    local found_count=0
    
    printf "${BLUE}→ Checking Q4 (Actors and Movie Count)...${NC}\n"
    
    # Array of expected entries
    declare -a expected=(
        '{"Ricardo Dar\u00edn": 18}'
        '{"Alejandro Awada": 7}'
        '{"Diego Peretti": 7}'
        '{"In\u00e9s Efron": 7}'
        '{"Leonardo Sbaraglia": 7}'
        '{"Valeria Bertuccelli": 7}'
        '{"Arturo Goetz": 6}'
        '{"Pablo Echarri": 6}'
        '{"Rafael Spregelburd": 6}'
        '{"Rodrigo de la Serna": 6}'
    )
    
    # Check file exists
    if [ ! -f "$file" ]; then
        printf "  ${RED}✗ File $file not found${NC}\n"
        return 1
    fi
    
    # Check line count - fail if not exact match
    local actual_count=$(grep -c . "$file")
    if [ "$actual_count" -ne $expected_count ]; then
        printf "  ${RED}✗ File has $actual_count records, expected exactly $expected_count${NC}\n"
        status=1
    fi
    
    # Check each expected entry without enforcing order
    for item in "${expected[@]}"; do
        # Escape special characters for grep
        if grep -q "$(echo "$item" | sed 's/[]\/$*.^[]/\\&/g')" "$file"; then
            ((found_count++))
        else
            printf "  ${RED}✗ Missing:${NC} $item\n"
            status=1
        fi
    done
    
    # Summary for this file
    if [ $status -eq 0 ]; then
        printf "  ${GREEN}✓ All $expected_count actor records found!${NC}\n"
    else
        printf "  ${RED}✗ Found $found_count of $expected_count expected records${NC}\n"
    fi
    
    return $status
}

check_q5() {
    local file=$1
    local status=0
    local expected_count=2
    local found_count=0
    
    printf "${BLUE}→ Checking Q5 (Sentiment Analysis)...${NC}\n"
    
    # Array of expected entries
    declare -a expected=(
        '{"sentiment": "POSITIVE", "average_ratio": 7443.9, "movie_count": 4431}'
        '{"sentiment": "NEGATIVE", "average_ratio": 3829.52, "movie_count": 3247}'
    )
    
    # Check file exists
    if [ ! -f "$file" ]; then
        printf "  ${RED}✗ File $file not found${NC}\n"
        return 1
    fi
    
    # Check line count - fail if not exact match
    local actual_count=$(grep -c . "$file")
    if [ "$actual_count" -ne $expected_count ]; then
        printf "  ${RED}✗ File has $actual_count records, expected exactly $expected_count${NC}\n"
        status=1
    fi
    
    # Check each expected line
    for item in "${expected[@]}"; do
        # Escape special characters for grep
        if grep -q "$(echo "$item" | sed 's/[]\/$*.^[]/\\&/g')" "$file"; then
            ((found_count++))
        else
            printf "  ${RED}✗ Missing:${NC} $item\n"
            status=1
        fi
    done
    
    # Summary for this file
    if [ $status -eq 0 ]; then
        printf "  ${GREEN}✓ All $expected_count sentiment analysis records found!${NC}\n"
    else
        printf "  ${RED}✗ Found $found_count of $expected_count expected records${NC}\n"
    fi
    
    return $status
}

# Main function
main() {
    printf "${BOLD}${YELLOW}===== Output Validation Script =====${NC}\n"
    
    # Show Q5 check status
    if $CHECK_Q5; then
        printf "Checking files in: ${BLUE}$OUTPUT_DIR${NC} (including Q5)\n\n"
    else
        printf "Checking files in: ${BLUE}$OUTPUT_DIR${NC} (Q5 checks disabled)\n\n"
    fi
    
    # Find all unique client numbers
    clients=$(find "$OUTPUT_DIR" -name "output_records_client_*_Q*.json" | grep -o "client_[0-9]\+" | sort -u | cut -d'_' -f2)
    
    if [ -z "$clients" ]; then
        printf "${RED}No client output files found!${NC}\n"
        exit 1
    fi
    
    total_clients=0
    passed_clients=0
    
    # Process each client
    for client in $clients; do
        ((total_clients++))
        client_passed=true
        
        printf "\n${BOLD}${YELLOW}===== Checking Client $client =====${NC}\n"
        
        # Check Q1
        q1_file="$OUTPUT_DIR/output_records_client_${client}_Q1.json"
        if ! check_q1 "$q1_file"; then
            client_passed=false
        fi
        
        printf "\n"
        
        # Check Q3
        q3_file="$OUTPUT_DIR/output_records_client_${client}_Q3.json"
        if ! check_q3 "$q3_file"; then
            client_passed=false
        fi
        
        printf "\n"
        
        # Check Q4
        q4_file="$OUTPUT_DIR/output_records_client_${client}_Q4.json"
        if ! check_q4 "$q4_file"; then
            client_passed=false
        fi
        
        # Check Q5 only if flag is provided
        if $CHECK_Q5; then
            printf "\n"
            
            # Check Q5
            q5_file="$OUTPUT_DIR/output_records_client_${client}_Q5.json"
            if ! check_q5 "$q5_file"; then
                client_passed=false
            fi
        fi
        
        # Display client summary
        if $client_passed; then
            printf "\n  ${GREEN}✓ CLIENT $client PASSED ALL TESTS${NC}\n"
            ((passed_clients++))
        else
            printf "\n  ${RED}✗ CLIENT $client FAILED${NC}\n"
        fi
    done
    
    # Display final summary
    printf "\n${BOLD}${YELLOW}===== Summary =====${NC}\n"
    if $CHECK_Q5; then
        printf "Checked: Q1, Q3, Q4, Q5\n"
    else
        printf "Checked: Q1, Q3, Q4 (Q5 skipped)\n"
    fi
    printf "Total clients checked: $total_clients\n"
    printf "${GREEN}Clients passed:${NC} $passed_clients\n"
    printf "${RED}Clients failed:${NC} $((total_clients - passed_clients))\n"
    
    if [ $passed_clients -eq $total_clients ]; then
        printf "\n${GREEN}${BOLD}✓ ALL CLIENTS PASSED ALL TESTS!${NC}\n"
        exit 0
    else
        printf "\n${RED}${BOLD}✗ SOME TESTS FAILED${NC}\n"
        exit 1
    fi
}

# Run the script
main