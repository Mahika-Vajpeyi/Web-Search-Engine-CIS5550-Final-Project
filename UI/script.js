
// document.getElementById('searchForm').addEventListener('submit', function(event) {
//     event.preventDefault();
//     const searchTerm = document.getElementById('searchInput').value;
//     displayResults(searchTerm);
// });


// function displayResults(searchTerm) {
//     const resultsDiv = document.getElementById('results');
//     resultsDiv.innerHTML = '';  // Clear previous results
//     if (!searchTerm) {
//         resultsDiv.innerHTML = '<p>Please enter a search term.</p>';
//         return;
//     }

//     fetch(`http://127.0.0.1:8003/search?query=${encodeURIComponent(searchTerm)}`)
//         .then(response => {
//             if (!response.ok) {
//                 throw new Error('Network response was not OK');
//             }

//             return response.json();  // Parse the JSON in the response

//         })
//         .then(data => {
//             console.log("Response data: ", data);  // Output the data to console
//             if (!data.data || data.data.length === 0) {
//                 resultsDiv.innerHTML = '<p>No results found.</p>';
//                 return;
//             }

//         //     // Display the result
//         //     const resultElement = document.createElement('div');
//         //     resultElement.innerHTML = `
//         //         <p>${data.result}</p>
//         //     `;
//         //     resultsDiv.appendChild(resultElement);
//         // })
//          // Display each result
//          data.data.forEach(item => {
//             const resultElement = document.createElement('div');
//             if(item.title && item.title.trim() !== ""){
//                 resultElement.innerHTML = `
//                     <h4>URL: <a href="${item.url}">${item.title}</a></h4>
//                 `;
//             }
//             else{
//                 resultElement.innerHTML = `
//                     <h4>URL: <a href="${item.url}">${item.url}</a></h4>
//                 `;
//             }
//             resultsDiv.appendChild(resultElement);
//         });
//     })
//     .catch(error => {
//         console.error('Failed to fetch data:', error);
//         resultsDiv.innerHTML = '<p>Error loading results.</p>';
//     });
// }

document.addEventListener('DOMContentLoaded', function() {
    // Restore state when the page loads
    const savedSearchTerm = localStorage.getItem('searchTerm');
    if (savedSearchTerm) {
        document.getElementById('searchInput').value = savedSearchTerm;
        displayResults(savedSearchTerm);  // Display the results for the saved search term
    }
});

document.getElementById('searchForm').addEventListener('submit', function(event) {
    event.preventDefault();
    const searchTerm = document.getElementById('searchInput').value;
    localStorage.setItem('searchTerm', searchTerm);  // Save the search term to local storage
    displayResults(searchTerm);
});

function displayResults(searchTerm) {
    const resultsDiv = document.getElementById('results');
    resultsDiv.innerHTML = '';  // Clear previous results
    if (!searchTerm) {
        resultsDiv.innerHTML = '<p>Please enter a search term.</p>';
        return;
    }

    fetch(`http://127.0.0.1:8003/search?query=${encodeURIComponent(searchTerm)}`)
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not OK');
            }
            return response.json();  // Parse the JSON in the response
        })
        .then(data => {
            console.log("Response data: ", data);  // Output the data to console
            if (!data.data || data.data.length === 0) {
                resultsDiv.innerHTML = '<p>No results found.</p>';
                return;
            }

            // Display each result
            data.data.forEach(item => {
                const resultElement = document.createElement('div');
                
                // resultElement.innerHTML = `
                //     <h4>URL: <a href="${item.url}">${item.title}</a></h4>
                // `;

                if (item.title && item.title.trim() !== "") {
                    resultElement.innerHTML = `
                        <h4>URL: <a href="${item.url}">${item.title}</a></h4>
                    `;
                } else {
                    resultElement.innerHTML = `
                        <h4>URL: <a href="${item.url}">${item.url}</a></h4>
                    `;
                }


                resultsDiv.appendChild(resultElement);
            });
        })
        .catch(error => {
            console.error('Failed to fetch data:', error);
            resultsDiv.innerHTML = '<p>Error loading results.</p>';
        });
}
