fn main() {
    println!("cargo:rustc-link-search=all=/data/repository/related_systems/qapla/lib/");
    println!("cargo:rustc-link-search=all=/usr/lib/mysql/");
    println!("cargo:rustc-link-search=all=/usr/lib64/");
    println!("cargo:rustc-link-search=all=/usr/lib/");
    println!("cargo:rustc-link-lib=static=refmonws");
    println!("cargo:rustc-link-lib=static=websubmitpol");
    println!("cargo:rustc-link-lib=static=qapla");
    println!("cargo:rustc-link-lib=static=mysqlparser");
    println!("cargo:rustc-link-lib=dylib=antlr3c");
    println!("cargo:rustc-link-lib=dylib=glib-2.0");
    println!("cargo:rustc-link-lib=dylib=mysqlclient");
    println!("cargo:rustc-link-lib=dylib=stdc++");
}
