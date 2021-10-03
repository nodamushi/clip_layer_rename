mod clip;
use regex::Regex;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 || args.len() > 3 {
        println!("renamelayer Input [Output]");
        std::process::exit(1);
    }

    if args[1] == "-v" {
        println!("v0.1.0");
        return;
    }

    let mut input_buf = PathBuf::from(&args[1]);
    let output = Path::new(&args[if args.len() == 2 { 1 } else { 2 }]);

    if !input_buf.exists() {
        println!("Error: {} file not found.", input_buf.display());
        std::process::exit(1);
    }

    // backup
    if input_buf == output {
        input_buf.set_extension("bk.clip");
        if let Err(e) = fs::rename(&args[1], &input_buf) {
            println!("Fail to create backup :{}", e);
        }
    }

    let re = Regex::new(r"レイヤー \d+").unwrap();
    if let Err(e) =
        clip::create_layer_renamed_clip_file(&input_buf, &output, "レイヤー ", |name| {
            re.is_match(name)
        })
    {
        return println!("Write Error: {}", e);
    }
}
